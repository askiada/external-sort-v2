package model

// Logger is a logger interface.
type Logger interface {
	SetLevel(lvl string)

	// Debug logs a message as debug.
	Debug(args ...interface{})
	// Debugf formats a message as debug.
	Debugf(format string, args ...interface{})
	// WithFieldsDebug logs a message as debug.
	WithFieldsDebug(fields map[string]interface{}, args ...interface{})
	// WithFieldsDebugf formats a message as debug.
	WithFieldsDebugf(fields map[string]interface{}, format string, args ...interface{})

	// Info logs a message as info.
	Info(args ...interface{})
	// Infof formats a message as info.
	Infof(format string, args ...interface{})
	// WithFieldsInfo logs a message as info.
	WithFieldsInfo(fields map[string]interface{}, args ...interface{})
	// WithFieldsInfof formats a message as info.
	WithFieldsInfof(fields map[string]interface{}, format string, args ...interface{})

	// Warn logs a message as warn.
	Warn(args ...interface{})
	// Warnf formats a message as warn.
	Warnf(format string, args ...interface{})
	// WithFieldsWarn logs a message as warn.
	WithFieldsWarn(fields map[string]interface{}, args ...interface{})
	// WithFieldsWarnf formats a message as warn.
	WithFieldsWarnf(fields map[string]interface{}, format string, args ...interface{})

	// Error logs a message as error.
	Error(args ...interface{})
	// Errorf formats a message as error.
	Errorf(format string, args ...interface{})
	// WithFieldsError logs a message as error.
	WithFieldsError(fields map[string]interface{}, args ...interface{})
	// WithFieldsErrorf formats a message as error.
	WithFieldsErrorf(fields map[string]interface{}, format string, args ...interface{})

	// Trace logs a message as trace.
	Trace(args ...interface{})
	// Tracef formats a message as trace.
	Tracef(format string, args ...interface{})
	// WithFieldsTrace logs a message as trace.
	WithFieldsTrace(fields map[string]interface{}, args ...interface{})
	// WithFieldsTracef formats a message as trace.
	WithFieldsTracef(fields map[string]interface{}, format string, args ...interface{})
}
