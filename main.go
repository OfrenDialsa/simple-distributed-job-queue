package main

import (
	"jobqueue/config"
	"jobqueue/delivery/graphql"
	_dataloader "jobqueue/delivery/graphql/dataloader"
	"jobqueue/delivery/graphql/mutation"
	"jobqueue/delivery/graphql/query"
	"jobqueue/delivery/graphql/schema"
	_htmx "jobqueue/delivery/htmx"
	"jobqueue/entity"
	"jobqueue/pkg/handler"
	"jobqueue/pkg/server"
	inmemrepo "jobqueue/repository/inmem"
	"jobqueue/service"
	"jobqueue/worker"
	"time"

	_graphql "github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"

	"github.com/labstack/echo"
	"github.com/labstack/echo/v4/middleware"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	setupLogger()
	defer zap.L().Sync()
	zap.L().Info("Starting Distributed Job Queue Engine with Zap Logger...")

	cfg := config.Data
	e := server.New(cfg.Server)
	e.Echo.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Format: "${remote_ip} ${time_rfc3339_nano} \"${method} ${path}\" ${status} ${bytes_out} \"${referer}\" \"${user_agent}\"\n",
	}))
	e.Echo.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET, echo.POST, echo.OPTIONS},
	}))

	//graphql schema
	opts := make([]_graphql.SchemaOpt, 0)
	opts = append(opts, _graphql.SubscribeResolverTimeout(10*time.Second))

	//initialize in mem database
	inMemDb := make(map[string]*entity.Job)

	//set job repository
	jobRepository := inmemrepo.
		NewJobRepository().
		SetInMemConnection(inMemDb).
		Build()
	dataloader := _dataloader.
		New().
		SetJobRepository(jobRepository).
		SetBatchFunction().
		Build()

	//set job worker
	jobWorker := worker.NewJobWorker().
		SetJobRepository(jobRepository).
		Build()

	//set job service
	jobService := service.NewJobService().
		SetJobRepository(jobRepository).
		SetJobWorker(jobWorker).
		Build()

	jobMutation := mutation.NewJobMutation(jobService, dataloader)
	jobQuery := query.NewJobQuery(jobService, dataloader)

	rootResolver := graphql.
		New().
		SetJobMutation(jobMutation).
		SetJobQuery(jobQuery).
		Build()

	graphqlSchema := _graphql.MustParseSchema(schema.String(), rootResolver, opts...)
	e.Echo.POST("/graphql",
		handler.GraphQLHandler(&relay.Handler{Schema: graphqlSchema}),
		dataloader.EchoMiddelware,
	)
	e.Echo.GET("/graphql",
		handler.GraphQLHandler(&relay.Handler{Schema: graphqlSchema}),
		dataloader.EchoMiddelware,
	)
	e.Echo.GET("/graphiql", handler.GraphiQLHandler)

	e.Echo.Renderer = _htmx.NewTemplateRenderer("web/htmx")
	htmxHandler := _htmx.NewHandler(jobService)

	e.Echo.GET("/jobqueue/dashboard", htmxHandler.Page)
	e.Echo.GET("/jobqueue/dashboard/status", htmxHandler.GetStatusSummary)
	e.Echo.GET("/jobqueue/dashboard/jobs", htmxHandler.GetJobsTable)
	e.Echo.GET("/jobqueue/dashboard/jobs/:id", htmxHandler.GetJobDetail)
	e.Echo.POST("/jobqueue/dashboard/jobs/retry/:id", htmxHandler.RetryDeadJob)
	e.Echo.POST("/jobqueue/dashboard/jobs/create", htmxHandler.CreateSimultaneousJobs)
	e.Echo.POST("/jobqueue/dashboard/jobs/unstable", htmxHandler.CreateUnstableJob)

	e.Echo.Logger.Fatal(e.Start())
}

func setupLogger() {
	configLogger := zap.NewDevelopmentConfig()
	configLogger.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	configLogger.DisableStacktrace = true
	logger, err := configLogger.Build()
	if err != nil {
		panic("Failed to initialize Zap logger: " + err.Error())
	}
	zap.ReplaceGlobals(logger)
}
