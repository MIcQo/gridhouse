package cmd

import (
	"fmt"
	"gridhouse/internal/stats"
	"runtime"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var str = `
Version: %s
Commit: %s
Build date: %s
GOOS: %s-%s`

var versionCmd = &cobra.Command{
	Use: "version",
	Run: func(_ *cobra.Command, _ []string) {
		fmt.Printf(
			str+"\n",
			stats.Version,
			stats.Commit,
			stats.BuildDate,
			runtime.GOOS,
			runtime.GOARCH,
		)
	},
}
