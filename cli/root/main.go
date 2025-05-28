package root

import (
	"context"
	"fmt"

	"aws-backup-route53/internal/awsutils"
	"aws-backup-route53/internal/fileutils"
	log "github.com/sirupsen/logrus"
)

type Options struct {
	S3Bucket    string `short:"b" long:"bucket" description:"The S3 bucket to save backup files to" env:"BUCKET" required:"true"`
	SaveLocally bool   `short:"l" long:"save-locally" description:"Save backup files locally"`
	Profile     string `short:"p" long:"profile" description:"The AWS CLI profile to use" env:"AWS_PROFILE"`
	Region      string `short:"r" long:"region" description:"The AWS region to use" env:"AWS_REGION"`
}

var Opts Options

func Process() {
	cfg := awsutils.LoadAwsDefaultConfig(Opts.Profile, Opts.Region)

	accountId, err := awsutils.GetAccountId(context.Background(), cfg)
	if err != nil {
		log.Fatalf("Unable to determine AWS account ID: %v", err)
	}

	allRecords, err := awsutils.ExportRoute53HostedZones(context.Background(), cfg, accountId)
	if err != nil {
		log.Fatalf("Unable to fetch Route 53 records: %v", err)
	}

	for zoneName, records := range allRecords {
		buf, err := fileutils.GenerateCSV(accountId, records)
		if err != nil {
			log.Fatalf("Unable to create CSV: %v", err)
		}

		key := fmt.Sprintf("Route53/%s/%s/backup.csv", accountId, zoneName)

		if Opts.SaveLocally {
			err := fileutils.WriteToFile(key, buf)
			if err != nil {
				log.Fatalf("Unable to write to file: %v", err)
			}
		} else {
			if err := awsutils.CopyToS3(context.Background(), cfg, Opts.S3Bucket, key, buf); err != nil {
				log.Fatalf("Unable to upload CSV to S3: %v", err)
			}

			log.Printf("Backup uploaded: s3://%s/%s", Opts.S3Bucket, key)
		}
	}
}
