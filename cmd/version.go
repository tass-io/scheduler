package cmd

import (
	"log"
	"runtime"

	"github.com/spf13/cobra"
)

type VersionInfo struct {
	LocalSchedulerVersion string
	GoVersion             string
	Compiler              string
	Platform              string
}

func (info *VersionInfo) String() string {
	return "{Local scheduler version: " + info.LocalSchedulerVersion + ", Go version: " +
		info.GoVersion + ", Compiler version: " + info.Compiler + ", Platform: " + info.Platform + "}"
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Version of the lcoal scheduler.",
	Long:  "Version of the lcoal scheduler.",
	Run: func(cmd *cobra.Command, args []string) {
		info := &VersionInfo{
			LocalSchedulerVersion: "v0.1.0",
			GoVersion:             runtime.Version(),
			Compiler:              runtime.Compiler,
			Platform:              runtime.GOOS + "/" + runtime.GOARCH,
		}
		log.Println(info.String())
	},
}

func init() {

}
