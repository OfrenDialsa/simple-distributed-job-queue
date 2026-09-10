package worker

import (
	"context"
	"fmt"
	"jobqueue/entity"
	_interface "jobqueue/interface"
	"math/rand"
	"time"
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
			fmt.Printf("[Worker] Job %s (unstable) FAILED on attempt %d/%d\n", job.ID, job.Attempts, maxAttempts)

			// if retry doesnt exceed max attempt
			if job.Attempts < maxAttempts {
				job.Status = entity.StatusFailed
				if err := w.repo.Update(ctx, job); err != nil {
					return fmt.Errorf("failed to update status to FAILED: %w", err)
				}

				backoff := delay * time.Duration(1<<(job.Attempts-1))
				fmt.Printf("[Worker] Retrying job %s in %v...\n", job.ID, backoff)
				time.Sleep(backoff)
				continue
			}

			// if retry attempt exceed max attemp -> move to Dead Letter Queue (DLQ)
			job.Status = entity.StatusDead
			if err := w.repo.Update(ctx, job); err != nil {
				return fmt.Errorf("failed to update status to DEAD: %w", err)
			}

			errDLQ := w.repo.SaveToDLQ(ctx, job, "Max retry attempts exceeded")
			if errDLQ != nil {
				return fmt.Errorf("failed to move job to DLQ: %w", errDLQ)
			}

			fmt.Printf("[DLQ] Job %s has been moved to Dead Letter Queue!\n", job.ID)
			return fmt.Errorf("job %s permanently failed after %d attempts", job.ID, job.Attempts)
		}

		job.Status = entity.StatusCompleted
		if err := w.repo.Update(ctx, job); err != nil {
			return fmt.Errorf("failed to update status to COMPLETED: %w", err)
		}

		fmt.Printf("[Worker] Job %s COMPLETED\n", job.ID)
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
