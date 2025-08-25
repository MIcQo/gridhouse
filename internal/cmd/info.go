package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
	"gridhouse/internal/stats"
	"os"
	"strings"
)

// InfoHandler handles the INFO command using live server stats
// Accepts a provider that exposes GetStats() *stats.OptimizedStatsManager
func InfoHandler(statsProvider interface{}) Handler {
	// Local interface to avoid import cycles for callers
	type provider interface {
		GetStats() *stats.OptimizedStatsManager
	}
	return func(args []resp.Value) (resp.Value, error) {
		var mgr *stats.OptimizedStatsManager
		if p, ok := statsProvider.(provider); ok && p != nil {
			mgr = p.GetStats()
		}
		// Build sections from snapshot (if mgr is nil, use zero-values)
		var snap stats.StatsSnapshot
		if mgr != nil {
			snap = mgr.GetSnapshot()
		}

		// Helper to build each section string
		buildServer := func() string {
			port := snap.Port
			role := snap.Role
			if role == "" {
				role = "master"
			}
			return "# Server\r\n" +
				fmt.Sprintf("redis_version:%s\r\n", snap.RedisVersion) +
				fmt.Sprintf("redis_mode:%s\r\n", "standalone") +
				fmt.Sprintf("os:%s\r\n", snap.OS) +
				fmt.Sprintf("tcp_port:%d\r\n", port) +
				fmt.Sprintf("process_id:%d\r\n", os.Getpid()) +
				fmt.Sprintf("process_supervised:%s\r\n", "unknown") +
				fmt.Sprintf("role:%s\r\n", role) +
				fmt.Sprintf("uptime_in_days:%.2f\r\n", snap.UptimeInDays) +
				fmt.Sprintf("uptime_in_seconds:%d\r\n", snap.Uptime)
		}
		buildClients := func() string {
			return "# Clients\r\n" +
				fmt.Sprintf("maxclients:%d\r\n", snap.MaxConnections) +
				fmt.Sprintf("connected_clients:%d\r\n", snap.ActiveConnections) +
				fmt.Sprintf("blocked_clients:%d\r\n", 0) + // TODO: since blocking commands are not part of gridhouse yet, we print 0
				fmt.Sprintf("tracking_clients:%d\r\n", 0) + // TODO: since tracing commands are not part of gridhouse yet, we print 0
				fmt.Sprintf("pubsub_clients:%d\r\n", 0) + // TODO: since pubsub commands are not part of gridhouse yet, we print 0
				fmt.Sprintf("watching_clients:%d\r\n", 0) // TODO: since watching commands are not part of gridhouse yet, we print 0
		}
		buildCPU := func() string {
			return "# CPU\r\n" +
				fmt.Sprintf("used_cpu_sys:%.6f\r\n", snap.UsedCPUSys) +
				fmt.Sprintf("used_cpu_user:%.6f\r\n", snap.UsedCPUUser) +
				fmt.Sprintf("used_cpu_sys_children:%.6f\r\n", snap.UsedCPUSysChildren) +
				fmt.Sprintf("used_cpu_user_children:%.6f\r\n", snap.UsedCPUUserChildren) +
				fmt.Sprintf("used_cpu_sys_main_thread:%.6f\r\n", snap.UsedCPUSysMainThread) +
				fmt.Sprintf("used_cpu_user_main_thread:%.6f\r\n", snap.UsedCPUUserMainThread)
		}
		buildMemory := func() string {
			return "# Memory\r\n" +
				fmt.Sprintf("used_memory:%d\r\n", snap.UsedMemory) +
				fmt.Sprintf("used_memory_peak:%d\r\n", snap.PeakMemory)
		}
		buildStats := func() string {
			return "# Stats\r\n" +
				fmt.Sprintf("total_connections_received:%d\r\n", snap.TotalConnectionsReceived) +
				fmt.Sprintf("total_commands_processed:%d\r\n", snap.TotalCommandsProcessed) +
				fmt.Sprintf("acl_access_denied_auth:%d\r\n", 0) + // TODO: we need to add counter for denied accesses
				fmt.Sprintf("total_reads_processed:%d\r\n", 0) + // TODO: we need to add counter for read commands
				fmt.Sprintf("total_writes_processed:%d\r\n", 0) // TODO: we need to add counter for write commands
		}
		buildCommands := func() string {
			b := strings.Builder{}
			b.WriteString("# Commands\r\n")
			if snap.CommandsByType != nil {
				for cmd, calls := range snap.CommandsByType {
					// We don't track latency here; keep 0 like Redis if unavailable
					b.WriteString(fmt.Sprintf("cmdstat_%s:calls=%d,usec=0,usec_per_call=0\r\n", strings.ToLower(cmd), calls))
				}
			}
			return b.String()
		}
		buildKeyspace := func() string {
			b := strings.Builder{}
			b.WriteString("# Keyspace\r\n")
			// If database stats available, print db0 aggregate only
			if len(snap.DatabaseKeys) == 0 {
				b.WriteString("db0:keys=0,expires=0,avg_ttl=0\r\n")
				return b.String()
			}
			var totalKeys int64
			var totalExpires int64
			for db, k := range snap.DatabaseKeys {
				_ = db
				totalKeys += k
				if snap.DatabaseExpires != nil {
					totalExpires += snap.DatabaseExpires[db]
				}
			}
			b.WriteString(fmt.Sprintf("db0:keys=%d,expires=%d,avg_ttl=0\r\n", totalKeys, totalExpires))
			return b.String()
		}

		section := ""
		if len(args) == 1 {
			section = strings.ToLower(args[0].Str)
		}

		var info strings.Builder
		switch section {
		case "SERVER", "server":
			info.WriteString(buildServer())
		case "CLIENTS", "clients":
			info.WriteString(buildClients())
		case "MEMORY", "memory":
			info.WriteString(buildMemory())
		case "STATS", "stats":
			info.WriteString(buildStats())
		case "COMMANDS", "commands":
			info.WriteString(buildCommands())
		case "KEYSPACE", "keyspace":
			info.WriteString(buildKeyspace())
		case "CPU", "cpu":
			info.WriteString(buildCPU())
		case "":
			info.WriteString(buildServer())
			info.WriteString("\r\n")
			info.WriteString(buildClients())
			info.WriteString("\r\n")
			info.WriteString(buildCPU())
			info.WriteString("\r\n")
			info.WriteString(buildMemory())
			info.WriteString("\r\n")
			info.WriteString(buildStats())
			info.WriteString("\r\n")
			info.WriteString(buildCommands())
			info.WriteString("\r\n")
			info.WriteString(buildKeyspace())
		default:
			// Unsupported section -> empty bulk string like Redis
			return resp.Value{Type: resp.BulkString, Str: ""}, nil
		}
		info.WriteString("\r\n")
		return resp.Value{Type: resp.BulkString, Str: info.String()}, nil
	}
}
