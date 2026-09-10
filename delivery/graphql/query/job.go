package query

import (
	"context"
	_dataloader "jobqueue/delivery/graphql/dataloader"
	"jobqueue/delivery/graphql/resolver"
	_interface "jobqueue/interface"
)

type JobQuery struct {
	jobService _interface.JobService
	dataloader *_dataloader.GeneralDataloader
}

func (q JobQuery) Jobs(ctx context.Context) ([]resolver.JobResolver, error) {
	jobs, err := q.jobService.GetAllJobs(ctx)
	if jobs == nil {
		return []resolver.JobResolver{}, nil
	}

	if err != nil {
		return nil, err
	}

	resolvers := make([]resolver.JobResolver, 0, len(jobs))
	for _, job := range jobs {
		if job != nil {
			resolvers = append(resolvers, resolver.JobResolver{
				Data:       *job,
				JobService: q.jobService,
				Dataloader: q.dataloader,
			})
		}
	}

	return resolvers, nil
}

func (q JobQuery) Job(ctx context.Context, args struct{ ID string }) (*resolver.JobResolver, error) {
	job, err := q.jobService.GetJobByID(ctx, args.ID)

	if job == nil {
		return &resolver.JobResolver{}, nil
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

func (q JobQuery) JobsByStatus(ctx context.Context, args struct{ Status string }) ([]resolver.JobResolver, error) {
	jobs, err := q.jobService.GetJobsByStatus(ctx, args.Status)
	if jobs == nil {
		return []resolver.JobResolver{}, nil
	}

	if err != nil {
		return nil, err
	}

	resolvers := make([]resolver.JobResolver, 0, len(jobs))
	for _, job := range jobs {
		if job != nil {
			resolvers = append(resolvers, resolver.JobResolver{
				Data:       *job,
				JobService: q.jobService,
				Dataloader: q.dataloader,
			})
		}
	}

	return resolvers, nil
}

func (q JobQuery) JobStatus(ctx context.Context) (*resolver.JobStatusResolver, error) {
	statusSummary, err := q.jobService.GetAllJobStatus(ctx)
	if err != nil {
		return nil, err
	}

	if statusSummary == nil {
		return &resolver.JobStatusResolver{}, nil
	}

	return &resolver.JobStatusResolver{
		Data:       *statusSummary,
		JobService: q.jobService,
		Dataloader: q.dataloader,
	}, nil
}

func NewJobQuery(jobService _interface.JobService,
	dataloader *_dataloader.GeneralDataloader) JobQuery {
	return JobQuery{
		jobService: jobService,
		dataloader: dataloader,
	}
}
