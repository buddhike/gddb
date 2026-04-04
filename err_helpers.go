package gddb

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// IsErrConditionalCheckFailed reports whether err is or wraps a DynamoDB
// [types.ConditionalCheckFailedException] (a conditional write failed).
func IsErrConditionalCheckFailed(err error) bool {
	var ccfe *types.ConditionalCheckFailedException
	ok := errors.As(err, &ccfe)
	return ok
}

// IsErrResourceInUse reports whether err is or wraps a DynamoDB
// [types.ResourceInUseException], indicating that the resource (e.g., table)
// is currently in use (such as during creation or deletion).
func IsErrResourceInUse(err error) bool {
	var inUse *types.ResourceInUseException
	return errors.As(err, &inUse)
}

// IsErrResourceNotFound reports whether err is or wraps a DynamoDB
// [types.ResourceNotFoundException], indicating that the resource (e.g., table)
// does not exist.
func IsErrResourceNotFound(err error) bool {
	var notfound *types.ResourceNotFoundException
	return errors.As(err, &notfound)
}
