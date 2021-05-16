package cmd

import (
	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	schttp "github.com/tass-io/scheduler/pkg/http"
	"github.com/tass-io/scheduler/pkg/initial"
	_ "github.com/tass-io/scheduler/pkg/middleware/lsds"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	"github.com/tass-io/scheduler/pkg/workflow"
	"go.uber.org/zap"
	"net/http"
	"os"
	"os/signal"
	"time"
)

var (
	rootCmd = &cobra.Command{
		Use:   "scheduler",
		Short: "scheduler",
		Long:  "scheduler",
		Run: func(cmd *cobra.Command, args []string) {
			k8sutils.Prepare()
			workflow.ManagerInit()
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

func init() {
	rootCmd.Flags().BoolP(env.Local, "l", false, "whether to use local file")
	viper.BindPFlag(env.Local, rootCmd.Flags().Lookup(env.Local))
	rootCmd.Flags().StringP(env.Port, "a", "8080", "the http port local scheduler exposed")
	viper.BindPFlag(env.Port, rootCmd.Flags().Lookup(env.Port))
	rootCmd.Flags().StringP(env.Policy, "p", "simple", "policy name to use")
	viper.BindPFlag(env.Policy, rootCmd.Flags().Lookup(env.Policy))
	rootCmd.Flags().StringP(env.SelfName, "s", "ubuntu", "if local is true, it is used to set selfName")
	viper.BindPFlag(env.SelfName, rootCmd.Flags().Lookup(env.SelfName))
	rootCmd.Flags().StringP(env.WorkflowPath, "w", "./workflow.yaml", "if local is true, it is used to init workflow by file")
	viper.BindPFlag(env.WorkflowPath, rootCmd.Flags().Lookup(env.WorkflowPath))
	rootCmd.Flags().StringP(env.WorkflowRuntimeFilePath, "r", "./workflowruntime.yaml", "if local is true, it is used to init workflow runtime by file")
	viper.BindPFlag(env.WorkflowRuntimeFilePath, rootCmd.Flags().Lookup(env.WorkflowRuntimeFilePath))
	rootCmd.Flags().DurationP(env.LSDSWait, "t", 200*time.Millisecond, "lsds wait a period of time for instance start")
	viper.BindPFlag(env.LSDSWait, rootCmd.Flags().Lookup(env.LSDSWait))
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(initial.InitCmd)
}
