package inmemrepo

import (
	"context"
	"jobqueue/entity"
	_interface "jobqueue/interface"
	custerr "jobqueue/pkg/errors"
	"sync"
)

type jobRepository struct {
	mu      sync.RWMutex
	inMemDb map[string]*entity.Job
	dlqDb   map[string]*entity.Job
}

// Save Job
func (t *jobRepository) Save(ctx context.Context, job *entity.Job) error {
	if job == nil {
		return custerr.ErrJobCannotBeNil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// add dereference/copy so pointer external pointer cant manipulate internal data
	jobCopy := *job
	t.inMemDb[job.ID] = &jobCopy
	return nil
}

// Update Job
func (t *jobRepository) Update(ctx context.Context, job *entity.Job) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.inMemDb[job.ID]; !exists {
		return custerr.ErrJobNotFound
	}

	t.inMemDb[job.ID] = job
	return nil
}

// Find Job By ID
func (t *jobRepository) FindByID(ctx context.Context, id string) (*entity.Job, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	job, exists := t.inMemDb[id]
	if !exists {
		return nil, custerr.ErrJobNotFound
	}

	// return copy for race condition safety
	jobCopy := *job
	return &jobCopy, nil
}

// Find by Key
func (t *jobRepository) FindByKey(ctx context.Context, key string) (*entity.Job, error) {
	if key == "" {
		return nil, nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, job := range t.inMemDb {
		if job.Key == key {
			jobCopy := *job
			return &jobCopy, nil
		}
	}

	return nil, nil
}

// FindAll Job
func (t *jobRepository) FindAll(ctx context.Context) ([]*entity.Job, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	jobs := make([]*entity.Job, 0, len(t.inMemDb))
	for _, job := range t.inMemDb {
		jobCopy := *job
		jobs = append(jobs, &jobCopy)
	}
	return jobs, nil
}

// Find By Status
func (t *jobRepository) FindByStatus(ctx context.Context, status string) ([]*entity.Job, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var jobs []*entity.Job
	for _, job := range t.inMemDb {
		if job.Status == status {
			jobCopy := *job
			jobs = append(jobs, &jobCopy)
		}
	}
	return jobs, nil
}

// Get Status Summary
func (t *jobRepository) GetStatusSummary(ctx context.Context) (*entity.JobStatus, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	summary := &entity.JobStatus{}

	for _, job := range t.inMemDb {
		switch job.Status {
		case entity.StatusPending:
			summary.Pending++
		case entity.StatusRunning:
			summary.Running++
		case entity.StatusFailed:
			summary.Failed++
		case entity.StatusCompleted:
			summary.Completed++
		case entity.StatusDead:
			summary.Dead++
		}
	}

	return summary, nil
}

// Save to DLQ for worker
func (t *jobRepository) SaveToDLQ(ctx context.Context, job *entity.Job, reason string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.dlqDb == nil {
		t.dlqDb = make(map[string]*entity.Job)
	}

	jobCopy := *job
	t.dlqDb[job.ID] = &jobCopy

	return nil
}

// Get From DLQ
func (t *jobRepository) GetFromDLQ(ctx context.Context, id string) (*entity.Job, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	job, exists := t.dlqDb[id]
	if !exists {
		return nil, custerr.ErrDLQJobNotFound
	}
	return job, nil
}

// Remove From DLQ
func (t *jobRepository) RemoveFromDLQ(ctx context.Context, id string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.dlqDb, id)
	return nil
}

// Initiator ...
type Initiator func(s *jobRepository) *jobRepository

// NewJobRepository ...
func NewJobRepository() Initiator {
	return func(q *jobRepository) *jobRepository {
		return q
	}
}

// SetInMemConnection set database client connection
func (i Initiator) SetInMemConnection(inMemDb map[string]*entity.Job) Initiator {
	return func(s *jobRepository) *jobRepository {
		i(s).inMemDb = inMemDb
		return s
	}
}

// Build ...
func (i Initiator) Build() _interface.JobRepository {
	repo := i(&jobRepository{})

	// fix: safety net if SetInMemConnection still nil
	if repo.inMemDb == nil {
		repo.inMemDb = make(map[string]*entity.Job)
	}
	return repo
}
