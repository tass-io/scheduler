package cmd

import (
	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/http"
	initial "github.com/tass-io/scheduler/pkg/initial"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	"github.com/tass-io/scheduler/pkg/workflow"
	"time"
)

var (
	rootCmd = &cobra.Command{
		Use:   "scheduler",
		Short: "scheduler",
		Long:  "scheduler",
		Run: func(cmd *cobra.Command, args []string) {
			k8sutils.Prepare()
			workflow.NewManager()
			r := gin.Default()
			http.RegisterRoute(r)
			r.Run()
		},
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.Flags().BoolP(env.Local, "l", false, "whether to use local file")
	viper.BindPFlag(env.Local, rootCmd.Flags().Lookup(env.Local))
	rootCmd.Flags().StringP(env.Policy, "p", "simple", "policy name to use")
	viper.BindPFlag(env.Policy, rootCmd.Flags().Lookup(env.Policy))
	rootCmd.Flags().StringP(env.SelfName, "s", "ubuntu", "if local is true, it is used to set selfName")
	viper.BindPFlag(env.SelfName, rootCmd.Flags().Lookup(env.SelfName))
	rootCmd.Flags().StringP(env.WorkflowPath, "w", "./workflow.yaml", "if local is true, it is used to init workflow by file")
	viper.BindPFlag(env.WorkflowPath, rootCmd.Flags().Lookup(env.WorkflowPath))
	rootCmd.Flags().StringP(env.WorkflowRuntimeFilePath, "r", "./workflowruntime.yaml", "if local is true, it is used to init workflow runtime by file")
	viper.BindPFlag(env.WorkflowRuntimeFilePath, rootCmd.Flags().Lookup(env.WorkflowRuntimeFilePath))
	rootCmd.Flags().DurationP(env.LSDSWait, "t", 200 * time.Millisecond, "lsds wait a period of time for instance start")
	viper.BindPFlag(env.LSDSWait, rootCmd.Flags().Lookup(env.LSDSWait))
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(initial.InitCmd)
}
