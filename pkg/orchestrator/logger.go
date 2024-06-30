package orchestrator

// Debug logs a message as debug.
func (o *Orchestrator) debug(args ...interface{}) {
	if o.logger != nil {
		o.logger.Debug(args...)
	}
}

func (o *Orchestrator) debugf(format string, args ...interface{}) {
	if o.logger != nil {
		o.logger.WithFieldsDebugf(o.defaultLoggerFields, format, args...)
	}
}

// WithFieldsDebug logs a message as debug.
func (o *Orchestrator) withFieldsDebug(fields map[string]interface{}, args ...interface{}) {
	if o.logger != nil {
		for k, v := range o.defaultLoggerFields {
			fields[k] = v
		}

		o.logger.WithFieldsDebug(fields, args...)
	}
}

// WithFieldsDebugf formats a message as debug.
func (o *Orchestrator) withFieldsDebugf(fields map[string]interface{}, format string, args ...interface{}) {
	if o.logger != nil {
		for k, v := range o.defaultLoggerFields {
			fields[k] = v
		}

		o.logger.WithFieldsDebugf(fields, format, args...)
	}
}

// Info logs a message as info.
func (o *Orchestrator) info(args ...interface{}) {
	if o.logger != nil {
		o.logger.WithFieldsInfo(o.defaultLoggerFields, args...)
	}
}

// Infof formats a message as info.
func (o *Orchestrator) infof(format string, args ...interface{}) {
	if o.logger != nil {
		o.logger.WithFieldsInfof(o.defaultLoggerFields, format, args...)
	}
}

// WithFieldsInfo logs a message as info.
func (o *Orchestrator) withFieldsInfo(fields map[string]interface{}, args ...interface{}) {
	if o.logger != nil {
		for k, v := range o.defaultLoggerFields {
			fields[k] = v
		}

		o.logger.WithFieldsInfo(fields, args...)
	}
}

// WithFieldsInfof formats a message as info.
func (o *Orchestrator) withFieldsInfof(fields map[string]interface{}, format string, args ...interface{}) {
	if o.logger != nil {
		for k, v := range o.defaultLoggerFields {
			fields[k] = v
		}

		o.logger.WithFieldsInfof(fields, format, args...)
	}
}

// Warn logs a message as warn.
func (o *Orchestrator) warn(args ...interface{}) {
	if o.logger != nil {
		o.logger.WithFieldsWarn(o.defaultLoggerFields, args...)
	}
}

// Warnf formats a message as warn.
func (o *Orchestrator) warnf(format string, args ...interface{}) {
	if o.logger != nil {
		o.logger.WithFieldsWarnf(o.defaultLoggerFields, format, args...)
	}
}

// WithFieldsWarn logs a message as warn.
func (o *Orchestrator) withFieldsWarn(fields map[string]interface{}, args ...interface{}) {
	if o.logger != nil {
		for k, v := range o.defaultLoggerFields {
			fields[k] = v
		}

		o.logger.WithFieldsWarn(fields, args...)
	}
}

// WithFieldsWarnf formats a message as warn.
func (o *Orchestrator) withFieldsWarnf(fields map[string]interface{}, format string, args ...interface{}) {
	if o.logger != nil {
		for k, v := range o.defaultLoggerFields {
			fields[k] = v
		}

		o.logger.WithFieldsWarnf(fields, format, args...)
	}
}

// Error logs a message as error.
func (o *Orchestrator) error(args ...interface{}) {
	if o.logger != nil {
		o.logger.WithFieldsError(o.defaultLoggerFields, args...)
	}
}

// Errorf formats a message as error.
func (o *Orchestrator) errorf(format string, args ...interface{}) {
	if o.logger != nil {
		o.logger.WithFieldsErrorf(o.defaultLoggerFields, format, args...)
	}
}

// WithFieldsError logs a message as error.
func (o *Orchestrator) withFieldsError(fields map[string]interface{}, args ...interface{}) {
	if o.logger != nil {
		for k, v := range o.defaultLoggerFields {
			fields[k] = v
		}

		o.logger.WithFieldsError(fields, args...)
	}
}

// WithFieldsErrorf formats a message as error.
func (o *Orchestrator) withFieldsErrorf(fields map[string]interface{}, format string, args ...interface{}) {
	if o.logger != nil {
		for k, v := range o.defaultLoggerFields {
			fields[k] = v
		}

		o.logger.WithFieldsErrorf(fields, format, args...)
	}
}

// Trace logs a message as trace.
func (o *Orchestrator) trace(args ...interface{}) {
	if o.logger != nil {
		o.logger.WithFieldsTrace(o.defaultLoggerFields, args...)
	}
}

// Tracef formats a message as trace.
func (o *Orchestrator) tracef(format string, args ...interface{}) {
	if o.logger != nil {
		o.logger.WithFieldsTracef(o.defaultLoggerFields, format, args...)
	}
}

// WithFieldsTrace logs a message as trace.
func (o *Orchestrator) withFieldsTrace(fields map[string]interface{}, args ...interface{}) {
	if o.logger != nil {
		for k, v := range o.defaultLoggerFields {
			fields[k] = v
		}

		o.logger.WithFieldsTrace(fields, args...)
	}
}

// WithFieldsTracef formats a message as trace.
func (o *Orchestrator) withFieldsTracef(fields map[string]interface{}, format string, args ...interface{}) {
	if o.logger != nil {
		for k, v := range o.defaultLoggerFields {
			fields[k] = v
		}

		o.logger.WithFieldsTracef(fields, format, args...)
	}
}
