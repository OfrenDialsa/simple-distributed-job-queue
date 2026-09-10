package htmx

import (
	"fmt"
	"net/http"

	_interface "jobqueue/interface"
	"jobqueue/pkg/ulid"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

type Handler struct {
	jobService _interface.JobService
}

func NewHandler(jobService _interface.JobService) *Handler {
	return &Handler{
		jobService: jobService,
	}
}

func (h *Handler) Page(c echo.Context) error {
	data := map[string]interface{}{
		"UniqueID": ulid.New(),
	}

	return c.Render(http.StatusOK, "index.html", data)
}

func (h *Handler) CreateSimultaneousJobs(c echo.Context) error {
	ctx := c.Request().Context()

	baseKey := c.FormValue("idempotency_key")
	if baseKey == "" {
		baseKey = ulid.New()
	}

	tasks := []string{
		c.FormValue("job1"),
		c.FormValue("job2"),
		c.FormValue("job3"),
	}

	for i, task := range tasks {
		if task == "" {
			continue
		}

		key := fmt.Sprintf("%s-%s-%d", baseKey, task, i+1)

		if _, err := h.jobService.Enqueue(ctx, task, key); err != nil {
			zap.L().Error("Failed to enqueue job", zap.String("task", task), zap.Error(err))
			return c.String(http.StatusInternalServerError, err.Error())
		}
	}

	jobs, _ := h.jobService.GetAllJobs(ctx)
	return c.Render(http.StatusOK, "table.html", jobs)
}

func (h *Handler) CreateUnstableJob(c echo.Context) error {
	ctx := c.Request().Context()
	key := fmt.Sprintf("unstable-job-%s", ulid.New())

	if _, err := h.jobService.Enqueue(ctx, "unstable-job", key); err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	jobs, _ := h.jobService.GetAllJobs(ctx)
	return c.Render(http.StatusOK, "table.html", jobs)
}

func (h *Handler) RetryDeadJob(c echo.Context) error {
	ctx := c.Request().Context()
	id := c.Param("id")

	job, err := h.jobService.RetryDeadJob(ctx, id)
	if err != nil {
		zap.L().Error("Failed to retry dead job", zap.String("job_id", id), zap.Error(err))
		return c.String(http.StatusBadRequest, err.Error())
	}

	zap.L().Info("Dead job retried successfully", zap.String("job_id", job.ID))

	jobs, err := h.jobService.GetAllJobs(ctx)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Render(http.StatusOK, "table.html", jobs)
}

func (h *Handler) GetStatusSummary(c echo.Context) error {
	ctx := c.Request().Context()
	stats, err := h.jobService.GetAllJobStatus(ctx)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Render(http.StatusOK, "status.html", stats)
}

func (h *Handler) GetJobsTable(c echo.Context) error {
	ctx := c.Request().Context()
	jobs, err := h.jobService.GetAllJobs(ctx)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Render(http.StatusOK, "table.html", jobs)
}

func (h *Handler) GetJobDetail(c echo.Context) error {
	ctx := c.Request().Context()
	id := c.Param("id")

	job, err := h.jobService.GetJobByID(ctx, id)
	if err != nil {
		return c.String(http.StatusNotFound, "Job tidak ditemukan")
	}

	return c.Render(http.StatusOK, "detail.html", job)
}

func StringPtr(s string) *string {
	return &s
}
