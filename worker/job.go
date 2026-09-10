package worker

import (
	"context"
	"fmt"
	"jobqueue/entity"
	_interface "jobqueue/interface"
	"math/rand"
	"time"

	"go.uber.org/zap"
)

type jobWorker struct {
	repo _interface.JobRepository
}

// initiator
type Initiator func(w *jobWorker) *jobWorker

func (w *jobWorker) ProcessJob(ctx context.Context, job *entity.Job) error {
	const maxAttempts = 3
	delay := 1 * time.Second

	for {
		job.Status = entity.StatusRunning
		if err := w.repo.Update(ctx, job); err != nil {
			zap.L().Error("Worker failed to update job status to RUNNING",
				zap.String("job_id", job.ID),
				zap.Error(err),
			)
			return fmt.Errorf("failed to update status to RUNNING: %w", err)
		}

		time.Sleep(500 * time.Millisecond)
		isFailed := false
		if job.Task == "unstable-job" {
			job.Attempts++

			if job.Attempts < 3 || rand.Float32() < 0.7 {
				isFailed = true
			}
		}

		// failed logic, retry and dlq
		if isFailed {
			zap.L().Warn("Job execution failed",
				zap.String("job_id", job.ID),
				zap.String("task", job.Task),
				zap.Int32("attempt", job.Attempts),
				zap.Int("max_attempts", maxAttempts),
			)

			// if retry doesnt exceed max attempt
			if job.Attempts < maxAttempts {
				job.Status = entity.StatusFailed
				if err := w.repo.Update(ctx, job); err != nil {
					zap.L().Error("Worker failed to update job status to FAILED",
						zap.String("job_id", job.ID),
						zap.Error(err),
					)
					return fmt.Errorf("failed to update status to FAILED: %w", err)
				}

				backoff := delay * time.Duration(1<<(job.Attempts-1))
				zap.L().Info("Retrying job with backoff",
					zap.String("job_id", job.ID),
					zap.Duration("backoff_duration", backoff),
					zap.Int32("next_attempt", job.Attempts+1),
				)
				time.Sleep(backoff)
				continue
			}

			// if retry attempt exceed max attemp -> move to Dead Letter Queue (DLQ)
			job.Status = entity.StatusDead
			if err := w.repo.Update(ctx, job); err != nil {
				zap.L().Error("Worker failed to update job status to DEAD",
					zap.String("job_id", job.ID),
					zap.Error(err),
				)
				return fmt.Errorf("failed to update status to DEAD: %w", err)
			}

			errDLQ := w.repo.SaveToDLQ(ctx, job, "Max retry attempts exceeded")
			if errDLQ != nil {
				zap.L().Error("Worker failed to move job to DLQ",
					zap.String("job_id", job.ID),
					zap.Error(errDLQ),
				)
				return fmt.Errorf("failed to move job to DLQ: %w", errDLQ)
			}

			zap.L().Warn("Job moved to Dead Letter Queue",
				zap.String("job_id", job.ID),
				zap.Int32("total_attempts", job.Attempts),
				zap.String("reason", "Max retry attempts exceeded"),
			)
			return fmt.Errorf("job %s permanently failed after %d attempts", job.ID, job.Attempts)
		}

		job.Status = entity.StatusCompleted
		if err := w.repo.Update(ctx, job); err != nil {
			zap.L().Error("Worker failed to update job status to COMPLETED",
				zap.String("job_id", job.ID),
				zap.Error(err),
			)
			return fmt.Errorf("failed to update status to COMPLETED: %w", err)
		}

		zap.L().Info("Job executed successfully",
			zap.String("job_id", job.ID),
			zap.String("task", job.Task),
			zap.String("status", string(job.Status)),
		)
		return nil
	}
}

// NewJobWorker ...
func NewJobWorker() Initiator {
	return func(w *jobWorker) *jobWorker {
		return w
	}
}

// SetJobRepository ...
func (i Initiator) SetJobRepository(jobRepository _interface.JobRepository) Initiator {
	return func(w *jobWorker) *jobWorker {
		w = i(w)
		w.repo = jobRepository
		return w
	}
}

// Build ...
func (i Initiator) Build() _interface.JobWorker {
	return i(&jobWorker{})
}
