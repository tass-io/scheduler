package cmd

import (
	"net/http"
	"os"
	"os/signal"
	"time"

	eventinit "github.com/tass-io/scheduler/pkg/event/init"
	schttp "github.com/tass-io/scheduler/pkg/http"
	"github.com/tass-io/scheduler/pkg/initial"
	middlewareinit "github.com/tass-io/scheduler/pkg/middleware/init"

	"github.com/tass-io/scheduler/pkg/runner/fnscheduler"
	"github.com/tass-io/scheduler/pkg/trace"
	"github.com/tass-io/scheduler/pkg/utils/k8sutils"
	"github.com/tass-io/scheduler/pkg/workflow"

	// init prometheus metrics
	_ "github.com/tass-io/scheduler/pkg/prom"
	// init logging config
	_ "github.com/tass-io/scheduler/pkg/utils/log"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"go.uber.org/zap"
)

var (
	rootCmd = &cobra.Command{
		Use:   "scheduler",
		Short: "scheduler",
		Long:  "scheduler",
		Run: func(cmd *cobra.Command, args []string) {
			// init tracing service config
			trace.Init()
			// init environment of k8s client
			k8sutils.Prepare()

			// init function scheduler which is responsible for scheduling the function to the appropriate process instance
			fnscheduler.Init()
			// init middleware framework for sync schedule,
			// middleware framework registers middlewares based on the startup parameters
			middlewareinit.Init()
			// init event framework for async schedule, event framework registers event handlers
			eventinit.Init()
			// init manager, which is responsible for fn scheduler, middleware framework and event framework
			workflow.InitManager()
			// start the manager based on the startup parameters
			workflow.GetManager().Start()

			r := gin.Default()
			schttp.RegisterRoute(r)
			server := &http.Server{
				Addr:    ":" + viper.GetString(env.Port),
				Handler: r,
			}

			close := make(chan os.Signal, 1)
			signal.Notify(close, os.Interrupt)

			go func() {
				<-close
				zap.S().Info("receive interrupt signal")
				if err := server.Close(); err != nil {
					zap.S().Error("Server Close:", err)
				}
			}()

			if err := server.ListenAndServe(); err != nil {
				if err == http.ErrServerClosed {
					zap.S().Info("Server closed under request")
				} else {
					zap.S().Errorw("Server closed unexpect", "err", err)
				}
			}
		},
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func SetArgs(args []string) {
	rootCmd.SetArgs(args)
}

func basicFlags() {
	rootCmd.Flags().StringP(env.Port, "a", "8080", "the http port local scheduler exposed")
	viper.BindPFlag(env.Port, rootCmd.Flags().Lookup(env.Port))

	rootCmd.Flags().BoolP(env.Local, "l", false, "whether to use local file")
	viper.BindPFlag(env.Local, rootCmd.Flags().Lookup(env.Local))
	rootCmd.Flags().StringP(env.SelfName, "s", "ubuntu",
		"if local is true, it is used to set selfName")
	viper.BindPFlag(env.SelfName, rootCmd.Flags().Lookup(env.SelfName))
	rootCmd.Flags().StringP(env.WorkflowPath, "w", "./workflow.yaml",
		"if local flag is true, it is used to init Workflow by file")
	viper.BindPFlag(env.WorkflowPath, rootCmd.Flags().Lookup(env.WorkflowPath))
	rootCmd.Flags().StringP(env.WorkflowRuntimeFilePath, "r", "./workflowruntime.yaml",
		"if local flag is true, it is used to init WorkflowRuntime by file")
	viper.BindPFlag(env.WorkflowRuntimeFilePath, rootCmd.Flags().Lookup(env.WorkflowRuntimeFilePath))
	rootCmd.Flags().StringSliceP(env.FuntionsPath, "f", []string{},
		"if local flag is true, it is used to init Function by file")
	viper.BindPFlag(env.FuntionsPath, rootCmd.Flags().Lookup(env.FuntionsPath))

	rootCmd.Flags().BoolP(env.Mock, "m", false, "whether to use mock instance")
	viper.BindPFlag(env.Mock, rootCmd.Flags().Lookup(env.Mock))

	rootCmd.Flags().BoolP(env.Prestart, "p", false, "enable/disable the prestart mode")
	viper.BindPFlag(env.Prestart, rootCmd.Flags().Lookup(env.Prestart))

	rootCmd.Flags().BoolP(env.StaticMiddleware, "i", false, "whether to use StaticMiddleware")
	viper.BindPFlag(env.StaticMiddleware, rootCmd.Flags().Lookup(env.StaticMiddleware))
	rootCmd.Flags().BoolP(env.QPSMiddleware, "q", false, "whether to use QPSMiddleware")
	viper.BindPFlag(env.QPSMiddleware, rootCmd.Flags().Lookup(env.QPSMiddleware))
	rootCmd.Flags().DurationP(env.TTL, "T", 20*time.Second, "set process default ttl")
	viper.BindPFlag(env.TTL, rootCmd.Flags().Lookup(env.TTL))
	rootCmd.Flags().DurationP(env.LSDSWait, "t", 200*time.Millisecond, "lsds wait a period of time for instance start")
	viper.BindPFlag(env.LSDSWait, rootCmd.Flags().Lookup(env.LSDSWait))
}

func policyFlags() {
	// policyFlag should not use single dash with a short letter
	rootCmd.Flags().String(env.RemoteCallPolicy, "simple", "policy name to use")
	viper.BindPFlag(env.RemoteCallPolicy, rootCmd.Flags().Lookup(env.RemoteCallPolicy))
	rootCmd.Flags().String(env.InstanceScorePolicy, "default", "settings about instance.Score")
	viper.BindPFlag(env.InstanceScorePolicy, rootCmd.Flags().Lookup(env.InstanceScorePolicy))
	rootCmd.Flags().String(env.CreatePolicy, "default", "settings about fnscheduler.canCreate")
	viper.BindPFlag(env.CreatePolicy, rootCmd.Flags().Lookup(env.CreatePolicy))
}

func storageFlags() {
	rootCmd.PersistentFlags().StringP(env.RedisIP, "I", "10.0.2.79", "redis ip to init function")
	viper.BindPFlag(env.RedisIP, rootCmd.PersistentFlags().Lookup(env.RedisIP))
	rootCmd.PersistentFlags().StringP(env.RedisPort, "P", "6379", "redis port to init function")
	viper.BindPFlag(env.RedisPort, rootCmd.PersistentFlags().Lookup(env.RedisPort))
	rootCmd.PersistentFlags().StringP(env.RedisPassword, "S", "", "redis password to init function")
	viper.BindPFlag(env.RedisPassword, rootCmd.PersistentFlags().Lookup(env.RedisPassword))
	rootCmd.PersistentFlags().Int32P(env.DefaultDb, "D", 0, "redis default db to init function")
	viper.BindPFlag(env.DefaultDb, rootCmd.PersistentFlags().Lookup(env.DefaultDb))
}

func tracingFlags() {
	rootCmd.Flags().String(env.TraceAgentHostPort, "106.15.225.249:6831", "set jaeger target")
	viper.BindPFlag(env.TraceAgentHostPort, rootCmd.Flags().Lookup(env.TraceAgentHostPort))
}

func flagsInit() {
	basicFlags()
	policyFlags()
	storageFlags()
	tracingFlags()
}

func commandsInit() {
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(initial.InitCmd)
}

func init() {
	flagsInit()
	commandsInit()
}
