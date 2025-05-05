package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/jessevdk/go-flags"
)

type Options struct {
	S3Bucket string `short:"s" long:"s3-bucket" description:"The S3 bucket to save the backup file to" required:"true"`
	Profile  string `short:"p" long:"profile" description:"The AWS CLI profile to use" env:"AWS_PROFILE" required:"true"`
}

var opts Options

type Record struct {
	AccountID string
	ZoneID    string
	ZoneName  string
	Name      string
	Type      string
	TTL       int64
	Value     string
}

func main() {
	_, err := flags.Parse(&opts)
	if err != nil {
		os.Exit(1)
	}

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx, config.WithSharedConfigProfile(opts.Profile))
	if err != nil {
		log.Fatalf("unable to load AWS config: %v", err)
	}

	accountId, err := getAccountId(ctx, cfg)
	if err != nil {
		log.Fatalf("unable to determine AWS account ID: %v", err)
	}

	records, err := exportRoute53HostedZones(ctx, cfg, accountId)
	if err != nil {
		log.Fatalf("unable to fetch Route 53 records: %v", err)
	}

	exportedAt := time.Now().Format(time.DateOnly + "_" + time.TimeOnly)

	buf, err := generateCSV(accountId, records)
	if err != nil {
		log.Fatalf("unable to create CSV: %v", err)
	}

	key := fmt.Sprintf("%s/route53-%s.csv", accountId, exportedAt)

	if err := copyToS3(ctx, cfg, opts.S3Bucket, key, buf); err != nil {
		log.Fatalf("Failed to upload CSV to S3: %v", err)
	}

	log.Printf("Backup uploaded: s3://%s/%s", opts.S3Bucket, key)
}

func getAccountId(ctx context.Context, cfg aws.Config) (string, error) {
	stsClient := sts.NewFromConfig(cfg)

	callerIdentity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", err
	}

	return *callerIdentity.Account, nil
}

func exportRoute53HostedZones(ctx context.Context, cfg aws.Config, accountID string) ([]Record, error) {
	client := route53.NewFromConfig(cfg)

	var zones []types.HostedZone
	var marker *string

	for {
		resp, err := client.ListHostedZones(ctx, &route53.ListHostedZonesInput{
			Marker: marker,
		})
		if err != nil {
			return nil, err
		}

		zones = append(zones, resp.HostedZones...)
		if !resp.IsTruncated {
			break
		}

		marker = resp.NextMarker
	}

	type result struct {
		records []Record
		err     error
	}

	recordsCh := make(chan result, len(zones))
	var wg sync.WaitGroup

	for _, zone := range zones {
		wg.Add(1)

		sem := make(chan struct{}, 5) // concurrency of 5
		sem <- struct{}{}
		go func(zone types.HostedZone) {
			defer wg.Done()

			var zoneRecords []Record
			var nextRecordName *string
			var nextRecordType types.RRType

			for {
				resp, err := client.ListResourceRecordSets(ctx, &route53.ListResourceRecordSetsInput{
					HostedZoneId:    zone.Id,
					StartRecordName: nextRecordName,
					StartRecordType: nextRecordType,
				})
				if err != nil {
					recordsCh <- result{nil, fmt.Errorf("zone %s: %w", *zone.Id, err)}
					return
				}

				re := regexp.MustCompile(`^/hostedzone/`)
				zoneId := re.ReplaceAllString(*zone.Id, "")
				zoneName := removeTrailingDot(*zone.Name)

				for _, recordSet := range resp.ResourceRecordSets {
					recordName := removeTrailingDot(*recordSet.Name)

					for _, v := range recordSet.ResourceRecords {
						recordValue := removeTrailingDot(*v.Value)

						zoneRecords = append(zoneRecords, Record{
							AccountID: accountID,
							ZoneID:    zoneId,
							ZoneName:  zoneName,
							Name:      recordName,
							Type:      strings.ToUpper(string(recordSet.Type)),
							TTL:       *recordSet.TTL,
							Value:     recordValue,
						})
					}
				}

				if !resp.IsTruncated {
					break
				}

				nextRecordName = resp.NextRecordName
				nextRecordType = resp.NextRecordType
			}

			recordsCh <- result{zoneRecords, nil}
		}(zone)
		<-sem
	}

	wg.Wait()
	close(recordsCh)

	var allRecords []Record

	for r := range recordsCh {
		if r.err != nil {
			log.Printf("error: %v", r.err)
			continue
		}

		allRecords = append(allRecords, r.records...)
	}

	return allRecords, nil
}

func removeTrailingDot(s string) string {
	re := regexp.MustCompile(`\.$`)

	return re.ReplaceAllString(s, "")
}

func generateCSV(accountId string, records []Record) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	w := csv.NewWriter(buf)

	if err := w.Write([]string{"Account ID", "Zone ID", "Zone Name", "Record Name", "Record Type", "Record TTL", "Record Value"}); err != nil {
		return nil, err
	}

	for _, record := range records {
		err := w.Write([]string{
			accountId,
			record.ZoneID,
			record.ZoneName,
			record.Name,
			record.Type,
			strconv.FormatInt(record.TTL, 10),
			record.Value,
		})
		if err != nil {
			return nil, err
		}
	}

	w.Flush()

	return buf, w.Error()
}

func copyToS3(ctx context.Context, cfg aws.Config, bucket, key string, buf *bytes.Buffer) error {
	client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(client)

	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(buf.Bytes()),
	})

	return err
}
