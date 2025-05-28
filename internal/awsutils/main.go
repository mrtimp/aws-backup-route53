package awsutils

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	log "github.com/sirupsen/logrus"
)

type Record struct {
	AccountID string
	ZoneID    string
	ZoneName  string
	Name      string
	Type      string
	TTL       string
	Value     string
}

type RecordGroupKey struct {
	Name string
	Type string
	TTL  string
}

func IsRunningViaLambda() bool {
	return os.Getenv("AWS_LAMBDA_RUNTIME_API") != ""
}

func LoadAwsDefaultConfig(profile string, region string) aws.Config {
	var optFns []func(options *config.LoadOptions) error

	if profile != "" {
		optFns = append(optFns, config.WithSharedConfigProfile(profile))
	}

	if region != "" {
		optFns = append(optFns, config.WithRegion(region))
	}

	log.Debugf("Loading AWS config: profile=%s, region=%s", profile, region)

	cfg, err := config.LoadDefaultConfig(context.Background(), optFns...)
	if err != nil {
		log.Fatal(err)
	}

	return cfg
}

func GetAccountId(ctx context.Context, cfg aws.Config) (string, error) {
	stsClient := sts.NewFromConfig(cfg)

	callerIdentity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", err
	}

	return *callerIdentity.Account, nil
}

func ExportRoute53HostedZones(ctx context.Context, cfg aws.Config, accountID string) (map[string][]Record, error) {
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
		zoneName string
		records  []Record
		err      error
	}

	recordsCh := make(chan result, len(zones))
	var wg sync.WaitGroup

	sem := make(chan struct{}, 5) // concurrency limiter

	for _, zone := range zones {
		wg.Add(1)
		sem <- struct{}{}
		go func(zone types.HostedZone) {
			defer wg.Done()
			defer func() { <-sem }()

			var zoneRecords []Record
			var nextRecordName *string
			var nextRecordType types.RRType

			re := regexp.MustCompile(`^/hostedzone/`)
			zoneId := re.ReplaceAllString(*zone.Id, "")
			zoneName := removeTrailingDot(*zone.Name)

			zoneGroups := make(map[RecordGroupKey][]string)

			for {
				resp, err := client.ListResourceRecordSets(ctx, &route53.ListResourceRecordSetsInput{
					HostedZoneId:    zone.Id,
					StartRecordName: nextRecordName,
					StartRecordType: nextRecordType,
				})
				if err != nil {
					recordsCh <- result{zoneName: zoneName, records: nil, err: fmt.Errorf("zone %s: %w", *zone.Id, err)}
					return
				}

				for _, recordSet := range resp.ResourceRecordSets {
					recordName := removeTrailingDot(*recordSet.Name)
					recordTTL := ""

					if recordSet.TTL != nil {
						recordTTL = strconv.FormatInt(*recordSet.TTL, 10)
					}

					key := RecordGroupKey{
						Name: recordName,
						Type: strings.ToUpper(string(recordSet.Type)),
						TTL:  recordTTL,
					}

					// ALIAS handling
					if recordSet.AliasTarget != nil {
						recordValue := fmt.Sprintf("ALIAS %s", removeTrailingDot(*recordSet.AliasTarget.DNSName))
						zoneGroups[key] = append(zoneGroups[key], recordValue)
						continue
					}

					for _, v := range recordSet.ResourceRecords {
						zoneGroups[key] = append(zoneGroups[key], removeTrailingDot(*v.Value))
					}
				}

				if !resp.IsTruncated {
					break
				}
				nextRecordName = resp.NextRecordName
				nextRecordType = resp.NextRecordType
			}

			for key, values := range zoneGroups {
				sort.Strings(values)
				zoneRecords = append(zoneRecords, Record{
					AccountID: accountID,
					ZoneID:    zoneId,
					ZoneName:  zoneName,
					Name:      key.Name,
					Type:      key.Type,
					TTL:       key.TTL,
					Value:     strings.Join(values, "\n"),
				})
			}

			// Sort records within this zone
			sort.Slice(zoneRecords, func(i, j int) bool {
				if zoneRecords[i].Type != zoneRecords[j].Type {
					return zoneRecords[i].Type < zoneRecords[j].Type
				}
				return zoneRecords[i].Name < zoneRecords[j].Name
			})

			recordsCh <- result{zoneName: zoneName, records: zoneRecords, err: nil}
		}(zone)
	}

	wg.Wait()
	close(recordsCh)

	zoneMap := make(map[string][]Record)

	for r := range recordsCh {
		if r.err != nil {
			log.Printf("error: %v", r.err)
			continue
		}
		zoneMap[r.zoneName] = r.records
	}

	return zoneMap, nil
}

func removeTrailingDot(s string) string {
	re := regexp.MustCompile(`\.$`)

	return re.ReplaceAllString(s, "")
}

func CopyToS3(ctx context.Context, cfg aws.Config, bucket, key string, buf *bytes.Buffer) error {
	client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(client)

	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(buf.Bytes()),
	})

	return err
}
