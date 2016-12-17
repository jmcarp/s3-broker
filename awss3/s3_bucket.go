package awss3

import (
	"bytes"
	"errors"
	"fmt"
	"text/template"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Bucket struct {
	s3svc  *s3.S3
	logger lager.Logger
}

func NewS3Bucket(
	s3svc *s3.S3,
	logger lager.Logger,
) *S3Bucket {
	return &S3Bucket{
		s3svc:  s3svc,
		logger: logger.Session("s3-bucket"),
	}
}

func (s *S3Bucket) Describe(bucketName, partition string) (BucketDetails, error) {
	getLocationInput := &s3.GetBucketLocationInput{
		Bucket: aws.String(bucketName),
	}
	s.logger.Debug("get-bucket-location", lager.Data{"input": getLocationInput})

	getLocationOutput, err := s.s3svc.GetBucketLocation(getLocationInput)
	if err != nil {
		s.logger.Error("aws-s3-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return BucketDetails{}, errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return BucketDetails{}, err
	}
	s.logger.Debug("get-bucket-location", lager.Data{"output": getLocationOutput})

	return s.buildBucketDetails(bucketName, *getLocationOutput.LocationConstraint, partition, nil), nil
}

func (s *S3Bucket) Create(bucketName string, bucketDetails BucketDetails) (string, error) {
	createBucketInput := s.buildCreateBucketInput(bucketName, bucketDetails)
	s.logger.Debug("create-bucket", lager.Data{"input": createBucketInput})

	createBucketOutput, err := s.s3svc.CreateBucket(createBucketInput)
	if err != nil {
		s.logger.Error("aws-s3-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return "", errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return "", err
	}
	s.logger.Debug("create-bucket", lager.Data{"output": createBucketOutput})

	if len(bucketDetails.Policy) > 0 {
		bucketDetails.BucketName = bucketName
		tmpl, err := template.New("policy").Parse(bucketDetails.Policy)
		if err != nil {
			s.logger.Error("aws-s3-error", err)
			return "", err
		}
		policy := bytes.Buffer{}
		err = tmpl.Execute(&policy, bucketDetails)
		if err != nil {
			s.logger.Error("aws-s3-error", err)
			return "", err
		}
		putPolicyInput := &s3.PutBucketPolicyInput{
			Bucket: aws.String(bucketDetails.BucketName),
			Policy: aws.String(policy.String()),
		}
		s.logger.Debug("put-bucket-policy", lager.Data{"input": putPolicyInput})
		putPolicyOutput, err := s.s3svc.PutBucketPolicy(putPolicyInput)
		if err != nil {
			s.logger.Error("aws-s3-error", err)
			if awsErr, ok := err.(awserr.Error); ok {
				return "", errors.New(awsErr.Code() + ": " + awsErr.Message())
			}
			return "", err
		}
		s.logger.Debug("put-bucket-policy", lager.Data{"output": putPolicyOutput})
	}

	return aws.StringValue(createBucketOutput.Location), nil
}

func (s *S3Bucket) Modify(bucketName string, bucketDetails BucketDetails) error {
	// TODO Implement modifx
	return nil
}

func (s *S3Bucket) Delete(bucketName string) error {
	err := s.clear(bucketName)
	if err != nil {
		return err
	}

	deleteBucketInput := &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	}
	s.logger.Debug("delete-bucket", lager.Data{"input": deleteBucketInput})

	deleteBucketOutput, err := s.s3svc.DeleteBucket(deleteBucketInput)
	if err != nil {
		s.logger.Error("aws-s3-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// AWS S3 returns a 400 if Bucket is not found
				if reqErr.StatusCode() == 400 || reqErr.StatusCode() == 404 {
					return ErrBucketDoesNotExist
				}
			}
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}
	s.logger.Debug("delete-bucket", lager.Data{"output": deleteBucketOutput})

	return nil
}

func (s3 *S3Bucket) buildBucketDetails(bucketName, region, partition string, attributes map[string]string) BucketDetails {
	return BucketDetails{
		BucketName: bucketName,
		Region:     region,
		ARN:        fmt.Sprintf("arn:%s:s3:::%s", partition, bucketName),
	}
}

func (s *S3Bucket) buildCreateBucketInput(bucketName string, bucketDetails BucketDetails) *s3.CreateBucketInput {
	createBucketInput := &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}
	return createBucketInput
}

func (s *S3Bucket) clear(bucketName string) error {
	err := s.clearObjects(bucketName)
	if err != nil {
		return err
	}

	return s.clearVersions(bucketName)
}

func (s *S3Bucket) clearObjects(bucketName string) error {
	var (
		marker  *string
		objects []*s3.ObjectIdentifier
	)

	for {
		listObjectsInput := &s3.ListObjectsInput{
			Bucket:  aws.String(bucketName),
			MaxKeys: aws.Int64(1000),
			Marker:  marker,
		}
		s.logger.Debug("list-objects", lager.Data{"input": listObjectsInput})

		listObjectsOutput, err := s.s3svc.ListObjects(listObjectsInput)
		if err != nil {
			s.logger.Error("aws-s3-error", err)
			if awsErr, ok := err.(awserr.Error); ok {
				return errors.New(awsErr.Code() + ": " + awsErr.Message())
			}
			return err
		}

		objects = []*s3.ObjectIdentifier{}
		for _, object := range listObjectsOutput.Contents {
			objects = append(objects, &s3.ObjectIdentifier{
				Key: object.Key,
			})
		}

		if len(objects) > 0 {
			deleteObjectsInput := &s3.DeleteObjectsInput{
				Bucket: aws.String(bucketName),
				Delete: &s3.Delete{Objects: objects},
			}
			s.logger.Debug("delete-versions", lager.Data{"input": deleteObjectsInput})

			_, err = s.s3svc.DeleteObjects(deleteObjectsInput)
			if err != nil {
				s.logger.Error("aws-s3-error", err)
				if awsErr, ok := err.(awserr.Error); ok {
					return errors.New(awsErr.Code() + ": " + awsErr.Message())
				}
				return err
			}
		}

		if aws.StringValue(listObjectsOutput.Marker) == "" {
			break
		}
	}

	return nil
}

func (s *S3Bucket) clearVersions(bucketName string) error {
	var (
		keyMarker       *string
		versionIdMarker *string
		objects         []*s3.ObjectIdentifier
	)

	for {
		listVersionsInput := &s3.ListObjectVersionsInput{
			Bucket:          aws.String(bucketName),
			KeyMarker:       keyMarker,
			VersionIdMarker: versionIdMarker,
		}
		s.logger.Debug("list-versions", lager.Data{"input": listVersionsInput})

		listVersionsOutput, err := s.s3svc.ListObjectVersions(listVersionsInput)
		if err != nil {
			s.logger.Error("aws-s3-error", err)
			if awsErr, ok := err.(awserr.Error); ok {
				return errors.New(awsErr.Code() + ": " + awsErr.Message())
			}
			return err
		}

		objects = []*s3.ObjectIdentifier{}
		for _, version := range listVersionsOutput.Versions {
			objects = append(objects, &s3.ObjectIdentifier{
				Key:       version.Key,
				VersionId: version.VersionId,
			})
		}

		if len(objects) > 0 {
			deleteObjectsInput := &s3.DeleteObjectsInput{
				Bucket: aws.String(bucketName),
				Delete: &s3.Delete{Objects: objects},
			}
			s.logger.Debug("delete-versions", lager.Data{"input": deleteObjectsInput})

			_, err = s.s3svc.DeleteObjects(deleteObjectsInput)
			if err != nil {
				s.logger.Error("aws-s3-error", err)
				if awsErr, ok := err.(awserr.Error); ok {
					return errors.New(awsErr.Code() + ": " + awsErr.Message())
				}
				return err
			}
		}

		keyMarker = listVersionsOutput.NextKeyMarker
		versionIdMarker = listVersionsOutput.VersionIdMarker
		if aws.StringValue(keyMarker) == "" && aws.StringValue(versionIdMarker) == "" {
			break
		}
	}

	return nil
}
