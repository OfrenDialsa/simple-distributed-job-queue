package mutation

import (
	"context"
	"errors"
	_dataloader "jobqueue/delivery/graphql/dataloader"
	"jobqueue/delivery/graphql/dto"
	"jobqueue/delivery/graphql/resolver"
	_interface "jobqueue/interface"
)

type JobMutation struct {
	jobService _interface.JobService
	dataloader *_dataloader.GeneralDataloader
}

func (q JobMutation) Enqueue(ctx context.Context, req dto.EnqueueRequest) (*resolver.JobResolver, error) {
	job, err := q.jobService.Enqueue(ctx, req.Task, req.Key)
	if job == nil {
		return nil, errors.New("failed to enqueue job: unexpected nil output")
	}

	if err != nil {
		return nil, err
	}

	return &resolver.JobResolver{
		Data:       *job,
		JobService: q.jobService,
		Dataloader: q.dataloader,
	}, nil
}

func (q JobMutation) RetryDeadJob(ctx context.Context, args struct{ ID string }) (*resolver.JobResolver, error) {
	job, err := q.jobService.RetryDeadJob(ctx, args.ID)
	if err != nil {
		return nil, err
	}

	if job == nil {
		return nil, errors.New("failed to retry dead job: unexpected nil output")
	}

	return &resolver.JobResolver{
		Data:       *job,
		JobService: q.jobService,
		Dataloader: q.dataloader,
	}, nil
}

// NewJobMutation to create new instance
func NewJobMutation(jobService _interface.JobService, dataloader *_dataloader.GeneralDataloader) JobMutation {
	return JobMutation{
		jobService: jobService,
		dataloader: dataloader,
	}
}
