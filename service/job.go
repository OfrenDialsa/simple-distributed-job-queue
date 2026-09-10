package service

import (
	"context"
	"errors"
	"fmt"
	"jobqueue/entity"
	_interface "jobqueue/interface"
	"jobqueue/pkg/ulid"
	"log"
)

type jobService struct {
	jobRepo _interface.JobRepository
	worker  _interface.JobWorker
}

// Initiator ...
type Initiator func(s *jobService) *jobService

func (q jobService) GetAllJobs(ctx context.Context) ([]*entity.Job, error) {
	if q.jobRepo == nil {
		return nil, errors.New("job repository is not initialized")
	}
	return q.jobRepo.FindAll(ctx)
}

func (q jobService) GetJobByID(ctx context.Context, id string) (*entity.Job, error) {
	if q.jobRepo == nil {
		return nil, errors.New("job repository is not initialized")
	}

	job, err := q.jobRepo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (q jobService) Enqueue(ctx context.Context, taskName, key string) (*entity.Job, error) {
	if q.jobRepo == nil {
		return nil, errors.New("job repository is not initialized")
	}

	if key != "" {
		existJob, err := q.jobRepo.FindByKey(ctx, key)
		if err != nil {
			return nil, err
		}

		if existJob != nil {
			log.Printf("[JobService] Idempotency hit! Returning existing job %s for key %s", existJob.ID, key)
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
		return nil, errors.New("job repository is not initialized")
	}
	return q.jobRepo.GetStatusSummary(ctx)
}

func (q jobService) GetJobsByStatus(ctx context.Context, status string) ([]*entity.Job, error) {
	if q.jobRepo == nil {
		return nil, errors.New("job repository is not initialized")
	}
	return q.jobRepo.FindByStatus(ctx, status)
}

func (s *jobService) RetryDeadJob(ctx context.Context, jobID string) (*entity.Job, error) {
	if s.jobRepo == nil {
		return nil, errors.New("job repository is not initialized")
	}

	deadJob, err := s.jobRepo.GetFromDLQ(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve job from DLQ: %w", err)
	}

	deadJob.Status = entity.StatusPending
	deadJob.Attempts = 0
	deadJob.Task = "retry-dead-job"

	if err := s.jobRepo.Update(ctx, deadJob); err != nil {
		return nil, fmt.Errorf("failed to reset job status: %w", err)
	}

	if err := s.jobRepo.RemoveFromDLQ(ctx, jobID); err != nil {
		return nil, fmt.Errorf("failed to remove job from DLQ: %w", err)
	}

	if s.worker != nil {
		go func(j *entity.Job) {
			if err := s.worker.ProcessJob(context.Background(), j); err != nil {
				log.Printf("[JobService] Error processing requeued job %s: %v", j.ID, err)
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
