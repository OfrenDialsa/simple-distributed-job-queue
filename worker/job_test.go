package worker_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"jobqueue/entity"
	_interface "jobqueue/interface"
	"jobqueue/pkg/ulid"
	inmemrepo "jobqueue/repository/inmem"
	"jobqueue/worker"

	"go.uber.org/zap"
)

func init() {
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)
}

func setupTestWorker() (_interface.JobWorker, _interface.JobRepository) {
	inMemDb := make(map[string]*entity.Job)
	repo := inmemrepo.NewJobRepository().SetInMemConnection(inMemDb).Build()
	jobWorker := worker.NewJobWorker().SetJobRepository(repo).SetBaseDelay(1 * time.Millisecond).Build()
	return jobWorker, repo
}

// Test 1: Menguji Eksekusi Job Normal hingga Status COMPLETED
func TestProcessJob_Success(t *testing.T) {
	jobWorker, repo := setupTestWorker()
	ctx := context.Background()

	job := &entity.Job{
		ID:       "job-normal-1",
		Task:     "normal-task",
		Status:   entity.StatusPending,
		Attempts: 0,
	}
	_ = repo.Save(ctx, job)

	err := jobWorker.ProcessJob(ctx, job)
	if err != nil {
		t.Fatalf("Expected job to succeed, got error: %v", err)
	}

	updatedJob, err := repo.FindByID(ctx, "job-normal-1")
	if err != nil {
		t.Fatalf("Failed to fetch job: %v", err)
	}

	if updatedJob.Status != entity.StatusCompleted {
		t.Errorf("Expected status COMPLETED, got: %s", updatedJob.Status)
	}
}

// Test 2: Menguji Logika Retry dan Pemindahan ke DLQ saat Max Attempts Terlampaui
func TestProcessJob_RetryAndDLQ(t *testing.T) {
	jobWorker, repo := setupTestWorker()
	ctx := context.Background()

	job := &entity.Job{
		ID:       ulid.New(),
		Task:     "always-fail-job",
		Status:   entity.StatusPending,
		Attempts: 0,
	}
	_ = repo.Save(ctx, job)

	err := jobWorker.ProcessJob(ctx, job)
	if err == nil {
		t.Fatal("Expected error due to DLQ transition, got nil")
	}

	updatedJob, err := repo.FindByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("Failed to fetch job: %v", err)
	}

	if updatedJob.Status != entity.StatusDead {
		t.Errorf("Expected status DEAD, got: %s", updatedJob.Status)
	}

	if updatedJob.Attempts != 3 {
		t.Errorf("Expected attempts count to be 3, got: %d", updatedJob.Attempts)
	}

	dlqJob, err := repo.GetFromDLQ(ctx, job.ID)
	if err != nil {
		t.Fatalf("Expected job in DLQ, got error: %v", err)
	}
	if dlqJob == nil {
		t.Fatal("Expected job to exist in DLQ")
	}
}

// Test 3: Menguji Pembatalan Context saat Worker Sedang Melakukan Delay/Sleep
func TestProcessJob_ContextCancellation(t *testing.T) {
	jobWorker, repo := setupTestWorker()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	job := &entity.Job{
		ID:       ulid.New(),
		Task:     "always-fail-job",
		Status:   entity.StatusPending,
		Attempts: 0,
	}
	_ = repo.Save(ctx, job)

	err := jobWorker.ProcessJob(ctx, job)
	if err == nil {
		t.Fatal("Expected context cancellation error, got nil")
	}
}

// Test 4: Concurrency Safety Test (Pengujian 100 Goroutines Eksekusi Bersamaan)
func TestProcessJob_ConcurrentSafety(t *testing.T) {
	jobWorker, repo := setupTestWorker()
	ctx := context.Background()

	const concurrentJobs = 100
	var wg sync.WaitGroup

	// Memproduksi 100 Jobs secara bersamaan
	for i := 1; i <= concurrentJobs; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			taskType := "normal-task"
			if id%2 == 0 {
				taskType = "unstable-job"
			}

			job := &entity.Job{
				ID:       fmt.Sprintf("job-concurrent-%d", id),
				Task:     taskType,
				Status:   entity.StatusPending,
				Attempts: 0,
			}

			if err := repo.Save(ctx, job); err != nil {
				t.Errorf("Failed to save job %s: %v", job.ID, err)
				return
			}

			// Jalankan worker secara konkuren
			_ = jobWorker.ProcessJob(ctx, job)
		}(i)
	}

	wg.Wait()

	// Verifikasi bahwa seluruh 100 jobs telah diproses
	allJobs, err := repo.FindAll(ctx)
	if err != nil {
		t.Fatalf("Failed to fetch all jobs: %v", err)
	}

	if len(allJobs) != concurrentJobs {
		t.Errorf("Expected %d jobs in repository, found %d", concurrentJobs, len(allJobs))
	}
}
