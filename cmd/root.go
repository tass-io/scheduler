package cmd

import (
	"github.com/spf13/cobra"
	"github.com/tass-io/scheduler/pkg/init"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
)

var rootCmd = &cobra.Command{
	Use:   "",
	Short: "scheduler",
	Long:  "scheduler",
	Run:   func(cmd *cobra.Command, args []string) {},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(init.InitCmd)
}
