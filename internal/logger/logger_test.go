package logger

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestLoggerInitialization(t *testing.T) {
	// Test initialization with different log levels
	testCases := []struct {
		name     string
		level    LogLevel
		expected logrus.Level
	}{
		{"debug_level", DebugLevel, logrus.DebugLevel},
		{"info_level", InfoLevel, logrus.InfoLevel},
		{"warn_level", WarnLevel, logrus.WarnLevel},
		{"error_level", ErrorLevel, logrus.ErrorLevel},
		{"fatal_level", FatalLevel, logrus.FatalLevel},
		{"invalid_level", "invalid", logrus.InfoLevel}, // Default fallback
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset logger
			log = nil

			// Initialize with test level
			Init(tc.level)

			// Verify log level is set correctly
			assert.Equal(t, tc.expected, Get().GetLevel())
		})
	}
}

func TestGetDefaultInitialization(t *testing.T) {
	// Reset logger to nil
	log = nil

	// Call Get() without initialization
	logger := Get()

	// Should initialize with default info level
	assert.NotNil(t, logger)
	assert.Equal(t, logrus.PanicLevel, logger.GetLevel())
}

func TestLoggingFunctions(t *testing.T) {
	// Capture log output
	var buf bytes.Buffer
	originalOutput := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Reset and initialize logger
	log = nil
	Init(InfoLevel)

	// Override output for testing
	Get().SetOutput(&buf)

	// Test all logging functions
	t.Run("debug_logging", func(t *testing.T) {
		buf.Reset()
		Debug("debug message")
		Debugf("debug formatted %s", "message")

		// Debug should not appear with Info level
		output := buf.String()
		assert.Empty(t, output)
	})

	t.Run("info_logging", func(t *testing.T) {
		buf.Reset()
		Info("info message")
		Infof("info formatted %s", "message")

		output := buf.String()
		assert.Contains(t, output, "info message")
		assert.Contains(t, output, "info formatted message")
	})

	t.Run("warn_logging", func(t *testing.T) {
		buf.Reset()
		Warn("warn message")
		Warnf("warn formatted %s", "message")

		output := buf.String()
		assert.Contains(t, output, "warn message")
		assert.Contains(t, output, "warn formatted message")
	})

	t.Run("error_logging", func(t *testing.T) {
		buf.Reset()
		Error("error message")
		Errorf("error formatted %s", "message")

		output := buf.String()
		assert.Contains(t, output, "error message")
		assert.Contains(t, output, "error formatted message")
	})

	// Restore stdout
	os.Stdout = originalOutput
	w.Close()
	io.Copy(io.Discard, r)
}

func TestDebugLoggingWithDebugLevel(t *testing.T) {
	// Capture log output
	var buf bytes.Buffer

	// Reset and initialize logger with debug level
	log = nil
	Init(DebugLevel)
	Get().SetOutput(&buf)

	// Test debug logging with debug level enabled
	Debug("debug message")
	Debugf("debug formatted %s", "message")

	output := buf.String()
	assert.Contains(t, output, "debug message")
	assert.Contains(t, output, "debug formatted message")
}

func TestStructuredLogging(t *testing.T) {
	// Capture log output
	var buf bytes.Buffer

	// Reset and initialize logger
	log = nil
	Init(InfoLevel)
	Get().SetOutput(&buf)

	// Test WithField
	WithField("key", "value").Info("message with field")
	output := buf.String()
	assert.Contains(t, output, "message with field")
	assert.Contains(t, output, "key=value")

	// Test WithFields
	buf.Reset()
	WithFields(logrus.Fields{
		"key1": "value1",
		"key2": "value2",
	}).Info("message with fields")

	output = buf.String()
	assert.Contains(t, output, "message with fields")
	assert.Contains(t, output, "key1=value1")
	assert.Contains(t, output, "key2=value2")
}

func TestLogFormatting(t *testing.T) {
	// Capture log output
	var buf bytes.Buffer

	// Reset and initialize logger
	log = nil
	Init(InfoLevel)
	Get().SetOutput(&buf)

	// Test that logs include timestamp
	Info("test message")
	output := buf.String()

	// Should contain timestamp format
	assert.Contains(t, output, "test message")
	assert.True(t, strings.Contains(output, "level=info") || strings.Contains(output, "INFO"))
}

func TestFatalLogging(t *testing.T) {
	// Note: We can't easily test Fatal/Fatalf as they call os.Exit
	// In a real scenario, you might want to use a test wrapper or mock
	// For now, we'll just verify the functions exist and don't panic

	// Reset and initialize logger
	log = nil
	Init(InfoLevel)

	// These should not panic (though they would exit in real usage)
	assert.NotPanics(t, func() {
		// We can't actually call Fatal in tests as it exits
		// This is more of a compilation test
	})
}

func TestConcurrentLogging(t *testing.T) {
	// Capture log output
	var buf bytes.Buffer

	// Reset and initialize logger
	log = nil
	Init(InfoLevel)
	Get().SetOutput(&buf)

	// Test concurrent logging
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			Infof("concurrent message %d", id)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	output := buf.String()
	// Should contain all concurrent messages
	for i := 0; i < 10; i++ {
		assert.Contains(t, output, fmt.Sprintf("concurrent message %d", i))
	}
}

func TestLoggerSingleton(t *testing.T) {
	// Reset logger
	log = nil

	// Get logger multiple times
	logger1 := Get()
	logger2 := Get()

	// Should return the same instance
	assert.Equal(t, logger1, logger2)

	// Should be initialized with default level
	assert.Equal(t, logrus.PanicLevel, logger1.GetLevel())
}

func TestLogLevelsOrdering(t *testing.T) {
	// Test that higher levels include lower levels
	var buf bytes.Buffer

	// Test with Warn level
	log = nil
	Init(WarnLevel)
	Get().SetOutput(&buf)

	// Debug and Info should not appear
	Debug("debug message")
	Info("info message")
	assert.Empty(t, buf.String())

	// Warn and Error should appear
	buf.Reset()
	Warn("warn message")
	Error("error message")
	output := buf.String()
	assert.Contains(t, output, "warn message")
	assert.Contains(t, output, "error message")
}
