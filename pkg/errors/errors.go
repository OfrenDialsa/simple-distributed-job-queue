package errors

import "errors"

var (
	ErrRepositoryNotInitialized = errors.New("job repository is not initialized")
	ErrJobNotFound              = errors.New("job not found")
	ErrDLQJobNotFound           = errors.New("failed to retrieve job from DLQ")
	ErrJobCannotBeNil           = errors.New("job cannot be nil")
	ErrUnexpectedNil            = errors.New("unexpected nil output")
	ErrUpdateJobStatusFailed    = errors.New("failed to reset job status in repository")
	ErrRemoveFromDLQFailed      = errors.New("failed to remove job from DLQ")
)
