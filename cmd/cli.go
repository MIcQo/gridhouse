package cmd

import (
	"gridhouse/internal/cli"
	"time"

	"github.com/spf13/cobra"
)

// cliCmd represents the CLI command
var cliCmd = &cobra.Command{
	Use:   "cli",
	Short: "Interactive GridHouse command-line interface",
	Long: `Interactive GridHouse command-line interface similar to redis-cli.
	
Connect to a GridHouse server and execute commands interactively or in batch mode.

Examples:
  gridhouse cli
  gridhouse cli --host 127.0.0.1 --port 6380
  gridhouse cli --eval "SET key value"
  gridhouse cli --file commands.txt`,
	Run: func(cmd *cobra.Command, args []string) {
		cli.RunCLI(&cli.CLIConfig{
			Host:     getStringFlag(cmd, "host", "127.0.0.1"),
			Port:     getIntFlag(cmd, "port", 6380),
			Password: getStringFlag(cmd, "password", ""),
			Database: getIntFlag(cmd, "db", 0),
			Timeout:  getDurationFlag(cmd, "timeout", 5*time.Second),
			TLS:      getBoolFlag(cmd, "tls"),
			Raw:      getBoolFlag(cmd, "raw"),
			Eval:     getStringFlag(cmd, "eval", ""),
			File:     getStringFlag(cmd, "file", ""),
			Pipe:     getBoolFlag(cmd, "pipe"),
		}, args)
	},
}

func init() {
	rootCmd.AddCommand(cliCmd)

	// Connection flags
	cliCmd.Flags().String("host", "127.0.0.1", "GridHouse server host")
	cliCmd.Flags().IntP("port", "p", 6380, "GridHouse server port")
	cliCmd.Flags().StringP("password", "a", "", "GridHouse server password")
	cliCmd.Flags().IntP("db", "d", 0, "Database number")
	cliCmd.Flags().Duration("timeout", 5*time.Second, "Connection timeout")
	cliCmd.Flags().Bool("tls", false, "Use TLS connection")

	// Input/output flags
	cliCmd.Flags().Bool("raw", false, "Use raw formatting for replies")
	cliCmd.Flags().String("eval", "", "Send specified command")
	cliCmd.Flags().String("file", "", "Execute commands from file")
	cliCmd.Flags().Bool("pipe", false, "Pipe mode - read from stdin and write to stdout")
}
