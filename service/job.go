package service

import (
	"context"
	"fmt"
	"jobqueue/entity"
	_interface "jobqueue/interface"
	custerr "jobqueue/pkg/errors"
	"jobqueue/pkg/ulid"

	"go.uber.org/zap"
)

type jobService struct {
	jobRepo _interface.JobRepository
	worker  _interface.JobWorker
}

// Initiator ...
type Initiator func(s *jobService) *jobService

func (q jobService) GetAllJobs(ctx context.Context) ([]*entity.Job, error) {
	if q.jobRepo == nil {
		return nil, custerr.ErrRepositoryNotInitialized
	}
	return q.jobRepo.FindAll(ctx)
}

func (q jobService) GetJobByID(ctx context.Context, id string) (*entity.Job, error) {
	if q.jobRepo == nil {
		return nil, custerr.ErrRepositoryNotInitialized
	}

	job, err := q.jobRepo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (q jobService) Enqueue(ctx context.Context, taskName, key string) (*entity.Job, error) {
	if q.jobRepo == nil {
		return nil, custerr.ErrRepositoryNotInitialized
	}

	if key != "" {
		existJob, err := q.jobRepo.FindByKey(ctx, key)
		if err != nil {
			return nil, err
		}

		if existJob != nil {
			zap.L().Info("Idempotency hit! Returning existing job",
				zap.String("job_id", existJob.ID),
				zap.String("key", key),
			)
			return existJob, nil
		}
	}

	newJob := &entity.Job{
		ID:       ulid.New(),
		Key:      key,
		Task:     taskName,
		Status:   entity.StatusPending,
		Attempts: 0,
	}

	err := q.jobRepo.Save(ctx, newJob)
	if err != nil {
		return nil, err
	}

	if q.worker != nil {
		go func(j *entity.Job) {
			if err := q.worker.ProcessJob(context.Background(), j); err != nil {
				_ = err
			}
		}(newJob)
	}

	return newJob, nil
}

func (q jobService) GetAllJobStatus(ctx context.Context) (*entity.JobStatus, error) {
	if q.jobRepo == nil {
		return nil, custerr.ErrRepositoryNotInitialized
	}
	return q.jobRepo.GetStatusSummary(ctx)
}

func (q jobService) GetJobsByStatus(ctx context.Context, status string) ([]*entity.Job, error) {
	if q.jobRepo == nil {
		return nil, custerr.ErrRepositoryNotInitialized
	}
	return q.jobRepo.FindByStatus(ctx, status)
}

func (s *jobService) RetryDeadJob(ctx context.Context, jobID string) (*entity.Job, error) {
	if s.jobRepo == nil {
		return nil, custerr.ErrRepositoryNotInitialized
	}

	deadJob, err := s.jobRepo.GetFromDLQ(ctx, jobID)
	if err != nil {
		return nil, custerr.ErrDLQJobNotFound
	}

	deadJob.Status = entity.StatusPending
	deadJob.Attempts = 0
	deadJob.Task = "retry-dead-job"

	if err := s.jobRepo.Update(ctx, deadJob); err != nil {
		return nil, fmt.Errorf("%w: %v", custerr.ErrUpdateJobStatusFailed, err)
	}

	if err := s.jobRepo.RemoveFromDLQ(ctx, jobID); err != nil {
		return nil, fmt.Errorf("%w: %v", custerr.ErrRemoveFromDLQFailed, err)
	}

	if s.worker != nil {
		go func(j *entity.Job) {
			if err := s.worker.ProcessJob(context.Background(), j); err != nil {
				zap.L().Error("Error processing requeued job",
					zap.String("job_id", j.ID),
					zap.Error(err),
				)
			}
		}(deadJob)
	}

	return deadJob, nil
}

// NewJobService ...
func NewJobService() Initiator {
	return func(s *jobService) *jobService {
		return s
	}
}

// SetJobRepository ...
func (i Initiator) SetJobRepository(jobRepository _interface.JobRepository) Initiator {
	return func(s *jobService) *jobService {
		s = i(s)
		s.jobRepo = jobRepository
		return s
	}
}

// SetJobWorker ...
func (i Initiator) SetJobWorker(worker _interface.JobWorker) Initiator {
	return func(s *jobService) *jobService {
		s = i(s)
		s.worker = worker
		return s
	}
}

// Build ...
func (i Initiator) Build() _interface.JobService {
	return i(&jobService{})
}
