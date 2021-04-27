package cmd

import (
	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/http"
	initial "github.com/tass-io/scheduler/pkg/initial"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	"github.com/tass-io/scheduler/pkg/workflow"
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
	rootCmd.Flags().BoolP("local", "l", false, "whether to use local file")
	viper.BindPFlag("local", rootCmd.Flags().Lookup("local"))
	rootCmd.Flags().StringP("policy", "p", "simple", "policy name to use")
	viper.BindPFlag("policy", rootCmd.Flags().Lookup("policy"))
	rootCmd.Flags().StringP("selfName", "s", "ubuntu", "if local is true, it is used to set selfName")
	viper.BindPFlag("selfName", rootCmd.Flags().Lookup("selfName"))
	rootCmd.Flags().StringP("workflowFilePath", "w", "./workflow.yaml", "if local is true, it is used to init workflow by file")
	viper.BindPFlag("workflowFilePath", rootCmd.Flags().Lookup("workflowFilePath"))
	rootCmd.Flags().StringP("workflowRuntimeFilePath", "r", "./workflowruntime.yaml", "if local is true, it is used to init workflow runtime by file")
	viper.BindPFlag("workflowRuntimeFilePath", rootCmd.Flags().Lookup("workflowRuntimeFilePath"))
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(initial.InitCmd)
}
