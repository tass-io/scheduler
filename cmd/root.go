package cmd

import (
	"github.com/spf13/cobra"
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
}
