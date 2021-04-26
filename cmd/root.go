package cmd

import (
	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
	"github.com/tass-io/scheduler/pkg/http"
	"github.com/tass-io/scheduler/pkg/init"
	_ "github.com/tass-io/scheduler/pkg/tools/k8sutils"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	_ "github.com/tass-io/scheduler/pkg/workflow"
)

var rootCmd = &cobra.Command{
	Use:   "scheduler",
	Short: "scheduler",
	Long:  "scheduler",
	Run:   func(cmd *cobra.Command, args []string) {
		r := gin.Default()
		http.RegisterRoute(r)
		r.Run()
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(init.InitCmd)
}
