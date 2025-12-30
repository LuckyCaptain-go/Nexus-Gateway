package security

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
)

// AWSIAMAuth handles AWS IAM-based authentication
type AWSIAMAuth struct {
	region     string
	roleArn    string
}

// NewAWSIAMAuth creates a new AWS IAM authenticator
func NewAWSIAMAuth(region, roleArn string) *AWSIAMAuth {
	return &AWSIAMAuth{
		region:  region,
		roleArn: roleArn,
	}
}

// GetCredentials retrieves AWS credentials using IAM role
func (a *AWSIAMAuth) GetCredentials(ctx context.Context) (aws.Credentials, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(a.region),
	)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// If role ARN is specified, use STS to assume role
	if a.roleArn != "" {
		stsProvider := stscreds.NewAssumeRoleProvider(stscreds.NewDefaultRoleProvider(cfg, a.roleArn), a.roleArn, func(opts *stscreds.AssumeRoleOptions) {
			opts.Duration = time.Hour
		})

		return aws.NewCredentialsCache(stsProvider), nil
	}

	return cfg.Credentials, nil
}

// ValidateConfig validates IAM configuration
func (a *AWSIAMAuth) ValidateConfig() error {
	if a.region == "" {
		return fmt.Errorf("AWS region is required")
	}
	return nil
}
