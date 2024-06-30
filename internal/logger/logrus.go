package logger

import (
	"os"

	"github.com/sirupsen/logrus"

	"github.com/askiada/external-sort-v2/internal/model"
)

type logrusLogger struct {
	log *logrus.Logger
}

func NewLogrus() model.Logger {
	return &logrusLogger{
		log: &logrus.Logger{
			Out:       os.Stdout,
			Formatter: new(logrus.TextFormatter),
			Hooks:     make(logrus.LevelHooks),
			Level:     logrus.InfoLevel,
		},
	}
}

// SetLevel sets the level. It accepts both UPPERCASE and lowercase. And PascalCase. And AnGrYcASe, for that matter.
// Invalid lvls, such as "Giraffe", will result in level set to Info.
func (l *logrusLogger) SetLevel(lvl string) {
	level, err := logrus.ParseLevel(lvl)
	if err != nil {
		level = logrus.InfoLevel
	}

	l.log.Infof("setting level to %s", level.String())
	l.log.SetLevel(level)
}

// Debug logs a message as debug.
func (l *logrusLogger) Debug(args ...interface{}) {
	l.log.Debug(args...)
}

// WithFieldsDebug logs a message as debug.
func (l *logrusLogger) WithFieldsDebug(fields map[string]interface{}, args ...interface{}) {
	l.log.WithFields(fields).Debug(args...)
}

// Debugf formats a message as debug.
func (l *logrusLogger) Debugf(format string, args ...interface{}) {
	l.log.Debugf(format, args...)
}

// WithFieldsDebugf formats a message as debug.
func (l *logrusLogger) WithFieldsDebugf(fields map[string]interface{}, format string, args ...interface{}) {
	l.log.WithFields(fields).Debugf(format, args...)
}

// Info logs a message as info.
func (l *logrusLogger) Info(args ...interface{}) {
	l.log.Info(args...)
}

// WithFieldsInfo logs a message as info.
func (l *logrusLogger) WithFieldsInfo(fields map[string]interface{}, args ...interface{}) {
	l.log.WithFields(fields).Info(args...)
}

// Infof formats a message as info.
func (l *logrusLogger) Infof(format string, args ...interface{}) {
	l.log.Infof(format, args...)
}

// WithFieldsInfof formats a message as info.
func (l *logrusLogger) WithFieldsInfof(fields map[string]interface{}, format string, args ...interface{}) {
	l.log.WithFields(fields).Infof(format, args...)
}

// Warn logs a message as warn.
func (l *logrusLogger) Warn(args ...interface{}) {
	l.log.Warn(args...)
}

// WithFieldsWarn logs a message as warn.
func (l *logrusLogger) WithFieldsWarn(fields map[string]interface{}, args ...interface{}) {
	l.log.WithFields(fields).Warn(args...)
}

// Warnf formats a message as warn.
func (l *logrusLogger) Warnf(format string, args ...interface{}) {
	l.log.Warnf(format, args...)
}

// WithFieldsWarnf formats a message as warn.
func (l *logrusLogger) WithFieldsWarnf(fields map[string]interface{}, format string, args ...interface{}) {
	l.log.WithFields(fields).Warnf(format, args...)
}

// Error logs a message as error.
func (l *logrusLogger) Error(args ...interface{}) {
	l.log.Error(args...)
}

// WithFieldsError logs a message as error.
func (l *logrusLogger) WithFieldsError(fields map[string]interface{}, args ...interface{}) {
	l.log.WithFields(fields).Error(args...)
}

// Errorf formats a message as error.
func (l *logrusLogger) Errorf(format string, args ...interface{}) {
	l.log.Errorf(format, args...)
}

// WithFieldsErrorf formats a message as error.
func (l *logrusLogger) WithFieldsErrorf(fields map[string]interface{}, format string, args ...interface{}) {
	l.log.WithFields(fields).Errorf(format, args...)
}

// Trace logs a message as trace.
func (l *logrusLogger) Trace(args ...interface{}) {
	l.log.Trace(args...)
}

// WithFieldsTrace logs a message as trace.
func (l *logrusLogger) WithFieldsTrace(fields map[string]interface{}, args ...interface{}) {
	l.log.WithFields(fields).Trace(args...)
}

// Tracef formats a message as trace.
func (l *logrusLogger) Tracef(format string, args ...interface{}) {
	l.log.Tracef(format, args...)
}

// WithFieldsTracef formats a message as trace.
func (l *logrusLogger) WithFieldsTracef(fields map[string]interface{}, format string, args ...interface{}) {
	l.log.WithFields(fields).Tracef(format, args...)
}

var _ model.Logger = &logrusLogger{}
