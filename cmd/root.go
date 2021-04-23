package cmd

import (
	"github.com/spf13/cobra"
	_ "github.com/tass-io/scheduler/pkg/tools"
)

var rootCmd = &cobra.Command{
	Use:   "",
	Short: "",
	Long:  "",
	Run:   func(cmd *cobra.Command, args []string) {},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
