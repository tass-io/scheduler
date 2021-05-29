package cmd

import (
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	eventinitial "github.com/tass-io/scheduler/pkg/event/initial"
	schttp "github.com/tass-io/scheduler/pkg/http"
	"github.com/tass-io/scheduler/pkg/initial"
	middlewareinitial "github.com/tass-io/scheduler/pkg/middleware/initial"
	_ "github.com/tass-io/scheduler/pkg/prom"
	"github.com/tass-io/scheduler/pkg/runner/fnscheduler"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	"github.com/tass-io/scheduler/pkg/workflow"
	"go.uber.org/zap"
)

var (
	rootCmd = &cobra.Command{
		Use:   "scheduler",
		Short: "scheduler",
		Long:  "scheduler",
		Run: func(cmd *cobra.Command, args []string) {
			k8sutils.Prepare()
			fnscheduler.FunctionSchedulerInit()
			workflow.ManagerInit()
			middlewareinitial.Initial()
			eventinitial.Initial()
			workflow.GetManagerIns().Start()
			r := gin.Default()
			schttp.RegisterRoute(r)
			server := &http.Server{
				Addr:    ":" + viper.GetString(env.Port),
				Handler: r,
			}

			quit := make(chan os.Signal)
			signal.Notify(quit, os.Interrupt)

			go func() {
				<-quit
				zap.S().Info("receive interrupt signal")
				if err := server.Close(); err != nil {
					zap.S().Error("Server Close:", err)
				}
			}()

			if err := server.ListenAndServe(); err != nil {
				if err == http.ErrServerClosed {
					zap.S().Info("Server closed under request")
				} else {
					zap.S().Error("Server closed unexpect")
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

func basicFlagInit() {
	rootCmd.Flags().BoolP(env.Local, "l", false, "whether to use local file")
	viper.BindPFlag(env.Local, rootCmd.Flags().Lookup(env.Local))
	rootCmd.Flags().StringP(env.Port, "a", "8080", "the http port local scheduler exposed")
	viper.BindPFlag(env.Port, rootCmd.Flags().Lookup(env.Port))
	rootCmd.Flags().String(env.RemoteCallPolicy, "simple", "policy name to use")
	viper.BindPFlag(env.RemoteCallPolicy, rootCmd.Flags().Lookup(env.RemoteCallPolicy))
	rootCmd.Flags().StringP(env.SelfName, "s", "ubuntu", "if local is true, it is used to set selfName")
	viper.BindPFlag(env.SelfName, rootCmd.Flags().Lookup(env.SelfName))
	rootCmd.Flags().StringP(env.WorkflowPath, "w", "./workflow.yaml", "if local is true, it is used to init workflow by file")
	viper.BindPFlag(env.WorkflowPath, rootCmd.Flags().Lookup(env.WorkflowPath))
	rootCmd.Flags().StringP(env.WorkflowRuntimeFilePath, "r", "./workflowruntime.yaml", "if local is true, it is used to init workflow runtime by file")
	viper.BindPFlag(env.WorkflowRuntimeFilePath, rootCmd.Flags().Lookup(env.WorkflowRuntimeFilePath))
	rootCmd.Flags().DurationP(env.LSDSWait, "t", 200*time.Millisecond, "lsds wait a period of time for instance start")
	viper.BindPFlag(env.LSDSWait, rootCmd.Flags().Lookup(env.LSDSWait))
	rootCmd.Flags().BoolP(env.Mock, "m", false, "whether to use mock instance")
	viper.BindPFlag(env.Mock, rootCmd.Flags().Lookup(env.Mock))
	rootCmd.Flags().BoolP(env.StaticMiddleware, "i", false, "whether to use StaticMiddleware")
	viper.BindPFlag(env.StaticMiddleware, rootCmd.Flags().Lookup(env.StaticMiddleware))
	rootCmd.Flags().BoolP(env.QPSMiddleware, "q", false, "whether to use QPSMiddleware")
	viper.BindPFlag(env.QPSMiddleware, rootCmd.Flags().Lookup(env.QPSMiddleware))
	rootCmd.Flags().DurationP(env.TTL, "T", 20*time.Second, "set process default ttl")
	viper.BindPFlag(env.TTL, rootCmd.Flags().Lookup(env.TTL))
}

func policyFlagInit() {
	// policyFlag should not use single dash with a short letter
	rootCmd.Flags().String(env.InstanceScorePolicy, "default", "settings about instance.Score")
	viper.BindPFlag(env.InstanceScorePolicy, rootCmd.Flags().Lookup(env.InstanceScorePolicy))
	rootCmd.Flags().String(env.CreatePolicy, "default", "settings about fnscheduler.canCreate")
	viper.BindPFlag(env.CreatePolicy, rootCmd.Flags().Lookup(env.CreatePolicy))
}

func initPersistentFlagInit() {
	rootCmd.PersistentFlags().StringP(env.RedisIp, "I", "10.0.0.96", "redis ip to init function")
	viper.BindPFlag(env.RedisIp, rootCmd.PersistentFlags().Lookup(env.RedisIp))
	rootCmd.PersistentFlags().StringP(env.RedisPort, "P", "30285", "redis port to init function")
	viper.BindPFlag(env.RedisPort, rootCmd.PersistentFlags().Lookup(env.RedisPort))
	rootCmd.PersistentFlags().StringP(env.RedisPassword, "S", "", "redis password to init function")
	viper.BindPFlag(env.RedisPassword, rootCmd.PersistentFlags().Lookup(env.RedisPassword))
	rootCmd.PersistentFlags().Int32P(env.DefaultDb, "D", 0, "redis default db to init function")
	viper.BindPFlag(env.DefaultDb, rootCmd.PersistentFlags().Lookup(env.DefaultDb))
}

func init() {
	basicFlagInit()
	policyFlagInit()
	initPersistentFlagInit()
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(initial.InitCmd)
}
