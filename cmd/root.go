/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"gridhouse/internal/aof"
	"gridhouse/internal/logger"
	"gridhouse/internal/persistence"
	"gridhouse/internal/server"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

const defaultReadBuffer = 256 * 1024
const defaultWriteBuffer = 0

// rootCmd represents base command when called without subcommands
var rootCmd = &cobra.Command{
	Use:   "gridhouse",
	Short: "A Redis-compatible in-memory database server",
	Long: `A Redis-compatible in-memory database server built in Go.
Supports basic Redis commands like SET, GET, PING, ECHO with TTL support.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Initialize logger
		logLevel := logger.LogLevel(getStringFlag(cmd, "log-level", "info"))
		logger.Init(logLevel)

		// Get persistence configuration from flags
		var persistConfig = &persistence.Config{
			Dir: getStringFlag(cmd, "dir", "./data"),
		}

		if getBoolFlag(cmd, "aof") {
			persistConfig.AOFEnabled = true
			persistConfig.AOFSyncMode = getAOFSyncMode(cmd)
			persistConfig.AOFRewriteConfig = getAOFRewriteConfig(cmd)
			persistConfig.RDBEnabled = getBoolFlag(cmd, "rdb")
		}

		if getBoolFlag(cmd, "rdb") {
			persistConfig.RDBEnabled = true
			persistConfig.RDBSaveConfig = &persistence.RDBSaveConfig{
				SaveInterval: time.Duration(getIntFlag(cmd, "save-interval", 300)) * time.Second,
				MinChanges:   getIntFlag(cmd, "min-changes", 1),
			}
		}

		// Create server with ultra-fast execution
		srv := server.New(server.Config{
			Addr:        getStringFlag(cmd, "port", ":6380"),
			Persistence: persistConfig,
			Password:    getStringFlag(cmd, "requirepass", ""),
			SlaveOf:     getStringFlag(cmd, "slaveof", ""),
			ReadBuffer:  getIntFlag(cmd, "read-buffer", defaultReadBuffer),
			WriteBuffer: getIntFlag(cmd, "write-buffer", defaultWriteBuffer),
		})

		// Start server
		if err := srv.Start(); err != nil {
			logger.Errorf("Failed to start server: %v", err)
			os.Exit(1)
		}

		logger.Infof("Server started on %s", srv.Addr())
		if persistConfig.RDBEnabled || persistConfig.AOFEnabled {
			logger.Infof("Persistence enabled - AOF: %v, RDB: %v", persistConfig.AOFEnabled, persistConfig.RDBEnabled)
		}

		// Wait for interrupt signal to gracefully shut down the server
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit

		logger.Info("Shutting down server...")
		if err := srv.Close(); err != nil {
			logger.Errorf("Error closing server: %v", err)
		}
	},
}

// Execute adds child commands to root and sets flags appropriately.
// Called by main.main(). Only needs to happen once to rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Persistence flags
	// general
	rootCmd.Flags().String("log-level", "info", "Log level (debug, info, warn, error, fatal)")

	// persistence
	rootCmd.Flags().String("dir", "./data", "Persistence directory")

	// AOF
	rootCmd.Flags().Bool("aof", false, "Enable AOF persistence")
	rootCmd.Flags().String("aof-sync", "everysec", "AOF sync mode (always, everysec, no)")
	rootCmd.Flags().Bool("aof-rewrite", true, "Enable AOF rewrite")
	rootCmd.Flags().Int64("aof-rewrite-growth-threshold", 64*1024*1024, "AOF rewrite growth threshold in bytes (default: 64MB)")
	rootCmd.Flags().Int64("aof-rewrite-min-size", 32*1024*1024, "AOF rewrite minimum size in bytes (default: 32MB)")
	rootCmd.Flags().Int("aof-rewrite-percentage", 100, "AOF rewrite percentage threshold (default: 100)")

	// RDB
	rootCmd.Flags().Bool("rdb", false, "Enable RDB persistence")
	rootCmd.Flags().Int("save-interval", 300, "RDB save interval in seconds")
	rootCmd.Flags().Int("min-changes", 1, "Minimum changes before RDB save")

	// server
	rootCmd.Flags().String("port", ":6380", "Server port")
	rootCmd.Flags().Int("write-buffer", defaultWriteBuffer, "Writer buffer size")
	rootCmd.Flags().Int("read-buffer", defaultReadBuffer, "Writer buffer size")

	// auth
	rootCmd.Flags().String("requirepass", "", "Password for AUTH command")

	// cluster/replica
	rootCmd.Flags().String("slaveof", "", "Replicate from master (format: host:port)")
}

// Helper functions for flag parsing
func getStringFlag(cmd *cobra.Command, name, defaultValue string) string {
	if value, err := cmd.Flags().GetString(name); err == nil && value != "" {
		return value
	}
	return defaultValue
}

func getBoolFlag(cmd *cobra.Command, name string) bool {
	if value, err := cmd.Flags().GetBool(name); err == nil {
		return value
	}
	return false
}

func getIntFlag(cmd *cobra.Command, name string, defaultValue int) int {
	if value, err := cmd.Flags().GetInt(name); err == nil {
		return value
	}
	return defaultValue
}

func getInt64Flag(cmd *cobra.Command, name string, defaultValue int64) int64 {
	if value, err := cmd.Flags().GetInt64(name); err == nil {
		return value
	}
	return defaultValue
}

func getAOFSyncMode(cmd *cobra.Command) aof.SyncMode {
	syncMode, _ := cmd.Flags().GetString("aof-sync")
	switch syncMode {
	case "always":
		return aof.Always
	case "no":
		return aof.No
	default:
		return aof.EverySec
	}
}

func getAOFRewriteConfig(cmd *cobra.Command) *aof.RewriteConfig {
	enabled, _ := cmd.Flags().GetBool("aof-rewrite")
	if !enabled {
		return &aof.RewriteConfig{Enabled: false}
	}

	return &aof.RewriteConfig{
		Enabled:           true,
		GrowthThreshold:   getInt64Flag(cmd, "aof-rewrite-growth-threshold", 64*1024*1024),
		MinRewriteSize:    getInt64Flag(cmd, "aof-rewrite-min-size", 32*1024*1024),
		RewritePercentage: getIntFlag(cmd, "aof-rewrite-percentage", 100),
	}
}
