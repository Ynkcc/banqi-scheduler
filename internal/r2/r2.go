package r2

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Presigner struct {
	client  *s3.Client
	bucket  string
	publicTTL time.Duration
}

func New(ctx context.Context, bucket string) (*Presigner, error) {
	// R2 兼容 S3 API，凭据与 endpoint 经标准环境变量提供：
	// AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_ENDPOINT_URL_S3 / AWS_REGION
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config (R2): %w", err)
	}
	if cfg.Region == "" {
		cfg.Region = "auto"
	}
	return &Presigner{
		client:    s3.NewFromConfig(cfg),
		bucket:    bucket,
		publicTTL: 1 * time.Hour,
	}, nil
}

// PresignPut 签发直传 URL；contentSHA256 为对象 sha256（hex），供 R2 端校验
func (p *Presigner) PresignPut(ctx context.Context, key, contentSHA256 string, length int64) (string, error) {
	req := &s3.PutObjectInput{
		Bucket:        aws.String(p.bucket),
		Key:           aws.String(key),
		ContentLength: aws.Int64(length),
	}
	if contentSHA256 != "" {
		req.ChecksumSHA256 = aws.String(contentSHA256)
	}
	ps := s3.NewPresignClient(p.client)
	out, err := ps.PresignPutObject(ctx, req, s3.WithPresignExpires(p.publicTTL))
	if err != nil {
		return "", fmt.Errorf("presign PUT %s: %w", key, err)
	}
	return out.URL, nil
}

func (p *Presigner) PresignGet(ctx context.Context, key string) (string, error) {
	ps := s3.NewPresignClient(p.client)
	out, err := ps.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(p.publicTTL))
	if err != nil {
		return "", fmt.Errorf("presign GET %s: %w", key, err)
	}
	return out.URL, nil
}

// EpisodeKey episodes/<network_sha>/<data_id>.jsonl.gz
func EpisodeKey(networkSha, dataID string) string {
	return fmt.Sprintf("episodes/%s/%s.jsonl.gz", networkSha, url.PathEscape(dataID))
}

// NetworkKey networks/<sha>.bin
func NetworkKey(sha string) string {
	return fmt.Sprintf("networks/%s.bin", sha)
}
