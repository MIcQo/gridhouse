package logger

import (
	"os"

	"github.com/sirupsen/logrus"
)

var log *logrus.Logger

// LogLevel represents the logging level
type LogLevel string

const (
	DebugLevel LogLevel = "debug"
	InfoLevel  LogLevel = "info"
	WarnLevel  LogLevel = "warn"
	ErrorLevel LogLevel = "error"
	PanicLevel LogLevel = "panic"
	FatalLevel LogLevel = "fatal"
)

// Init initializes the logger with the specified level
func Init(level LogLevel) {
	log = logrus.New()

	// Set output to stdout
	log.SetOutput(os.Stdout)

	// Set formatter to include timestamp
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})

	// Set log level
	switch level {
	case DebugLevel:
		log.SetLevel(logrus.DebugLevel)
	case InfoLevel:
		log.SetLevel(logrus.InfoLevel)
	case WarnLevel:
		log.SetLevel(logrus.WarnLevel)
	case ErrorLevel:
		log.SetLevel(logrus.ErrorLevel)
	case PanicLevel:
		log.SetLevel(logrus.PanicLevel)
	case FatalLevel:
		log.SetLevel(logrus.FatalLevel)
	default:
		log.SetLevel(logrus.InfoLevel)
	}
}

// Get returns the logger instance
func Get() *logrus.Logger {
	if log == nil {
		// Initialize with default level if not initialized
		Init(PanicLevel)
	}
	return log
}

// Debug logs a debug message
func Debug(args ...interface{}) {
	Get().Debug(args...)
}

// Debugf logs a formatted debug message
func Debugf(format string, args ...interface{}) {
	Get().Debugf(format, args...)
}

// Info logs an info message
func Info(args ...interface{}) {
	Get().Info(args...)
}

// Infof logs a formatted info message
func Infof(format string, args ...interface{}) {
	Get().Infof(format, args...)
}

// Warn logs a warning message
func Warn(args ...interface{}) {
	Get().Warn(args...)
}

// Warnf logs a formatted warning message
func Warnf(format string, args ...interface{}) {
	Get().Warnf(format, args...)
}

// Error logs an error message
func Error(args ...interface{}) {
	Get().Error(args...)
}

// Errorf logs a formatted error message
func Errorf(format string, args ...interface{}) {
	Get().Errorf(format, args...)
}

// Fatal logs a fatal message and exits
func Fatal(args ...interface{}) {
	Get().Fatal(args...)
}

// Fatalf logs a formatted fatal message and exits
func Fatalf(format string, args ...interface{}) {
	Get().Fatalf(format, args...)
}

// WithField returns a logger with a field
func WithField(key string, value interface{}) *logrus.Entry {
	return Get().WithField(key, value)
}

// WithFields returns a logger with multiple fields
func WithFields(fields logrus.Fields) *logrus.Entry {
	return Get().WithFields(fields)
}
