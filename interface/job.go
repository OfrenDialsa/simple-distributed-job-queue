package _interface

import (
	"context"
	"jobqueue/entity"
)

type JobService interface {
	GetAllJobs(ctx context.Context) ([]*entity.Job, error)
	Enqueue(ctx context.Context, taskName, key string) (*entity.Job, error)

	//added methods
	GetJobByID(ctx context.Context, id string) (*entity.Job, error)
	GetAllJobStatus(ctx context.Context) (*entity.JobStatus, error)
	GetJobsByStatus(ctx context.Context, status string) ([]*entity.Job, error)
	RetryDeadJob(ctx context.Context, jobID string) (*entity.Job, error)
}

type JobRepository interface {
	Save(ctx context.Context, job *entity.Job) error
	FindByID(ctx context.Context, id string) (*entity.Job, error)
	FindAll(ctx context.Context) ([]*entity.Job, error)

	//added methods
	Update(ctx context.Context, job *entity.Job) error
	FindByKey(ctx context.Context, key string) (*entity.Job, error)
	FindByStatus(ctx context.Context, status string) ([]*entity.Job, error)
	GetStatusSummary(ctx context.Context) (*entity.JobStatus, error)
	SaveToDLQ(ctx context.Context, job *entity.Job, reason string) error
	GetFromDLQ(ctx context.Context, id string) (*entity.Job, error)
	RemoveFromDLQ(ctx context.Context, id string) error
}

type JobWorker interface {
	ProcessJob(ctx context.Context, job *entity.Job) error
}
