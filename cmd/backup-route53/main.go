package main

import (
	"aws-backup-route53/cli/root"
	"aws-backup-route53/internal/awsutils"
	"context"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/jessevdk/go-flags"
)

func main() {
	if !awsutils.IsRunningViaLambda() {
		_, err := flags.Parse(&root.Opts)
		if err != nil {
			os.Exit(1)
		}

		root.Process()

		os.Exit(0)
	}

	lambda.Start(lambdaHandler)
}

func lambdaHandler(ctx context.Context) {
	root.Process()
}
