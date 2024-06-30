package chunksorter

// Debug logs a message as debug.
func (cs *ChunkSorter) debug(args ...interface{}) {
	if cs.logger != nil {
		cs.logger.Debug(args...)
	}
}

func (cs *ChunkSorter) debugf(format string, args ...interface{}) {
	if cs.logger != nil {
		cs.logger.WithFieldsDebugf(cs.defaultLoggerFields, format, args...)
	}
}

// WithFieldsDebug logs a message as debug.
func (cs *ChunkSorter) withFieldsDebug(fields map[string]interface{}, args ...interface{}) {
	if cs.logger != nil {
		for k, v := range cs.defaultLoggerFields {
			fields[k] = v
		}

		cs.logger.WithFieldsDebug(fields, args...)
	}
}

// WithFieldsDebugf formats a message as debug.
func (cs *ChunkSorter) withFieldsDebugf(fields map[string]interface{}, format string, args ...interface{}) {
	if cs.logger != nil {
		for k, v := range cs.defaultLoggerFields {
			fields[k] = v
		}

		cs.logger.WithFieldsDebugf(fields, format, args...)
	}
}

// Info logs a message as info.
func (cs *ChunkSorter) info(args ...interface{}) {
	if cs.logger != nil {
		cs.logger.WithFieldsInfo(cs.defaultLoggerFields, args...)
	}
}

// Infof formats a message as info.
func (cs *ChunkSorter) infof(format string, args ...interface{}) {
	if cs.logger != nil {
		cs.logger.WithFieldsInfof(cs.defaultLoggerFields, format, args...)
	}
}

// WithFieldsInfo logs a message as info.
func (cs *ChunkSorter) withFieldsInfo(fields map[string]interface{}, args ...interface{}) {
	if cs.logger != nil {
		for k, v := range cs.defaultLoggerFields {
			fields[k] = v
		}

		cs.logger.WithFieldsInfo(fields, args...)
	}
}

// WithFieldsInfof formats a message as info.
func (cs *ChunkSorter) withFieldsInfof(fields map[string]interface{}, format string, args ...interface{}) {
	if cs.logger != nil {
		for k, v := range cs.defaultLoggerFields {
			fields[k] = v
		}

		cs.logger.WithFieldsInfof(fields, format, args...)
	}
}

// Warn logs a message as warn.
func (cs *ChunkSorter) warn(args ...interface{}) {
	if cs.logger != nil {
		cs.logger.WithFieldsWarn(cs.defaultLoggerFields, args...)
	}
}

// Warnf formats a message as warn.
func (cs *ChunkSorter) warnf(format string, args ...interface{}) {
	if cs.logger != nil {
		cs.logger.WithFieldsWarnf(cs.defaultLoggerFields, format, args...)
	}
}

// WithFieldsWarn logs a message as warn.
func (cs *ChunkSorter) withFieldsWarn(fields map[string]interface{}, args ...interface{}) {
	if cs.logger != nil {
		for k, v := range cs.defaultLoggerFields {
			fields[k] = v
		}

		cs.logger.WithFieldsWarn(fields, args...)
	}
}

// WithFieldsWarnf formats a message as warn.
func (cs *ChunkSorter) withFieldsWarnf(fields map[string]interface{}, format string, args ...interface{}) {
	if cs.logger != nil {
		for k, v := range cs.defaultLoggerFields {
			fields[k] = v
		}

		cs.logger.WithFieldsWarnf(fields, format, args...)
	}
}

// Error logs a message as error.
func (cs *ChunkSorter) error(args ...interface{}) {
	if cs.logger != nil {
		cs.logger.WithFieldsError(cs.defaultLoggerFields, args...)
	}
}

// Errorf formats a message as error.
func (cs *ChunkSorter) errorf(format string, args ...interface{}) {
	if cs.logger != nil {
		cs.logger.WithFieldsErrorf(cs.defaultLoggerFields, format, args...)
	}
}

// WithFieldsError logs a message as error.
func (cs *ChunkSorter) withFieldsError(fields map[string]interface{}, args ...interface{}) {
	if cs.logger != nil {
		for k, v := range cs.defaultLoggerFields {
			fields[k] = v
		}

		cs.logger.WithFieldsError(fields, args...)
	}
}

// WithFieldsErrorf formats a message as error.
func (cs *ChunkSorter) withFieldsErrorf(fields map[string]interface{}, format string, args ...interface{}) {
	if cs.logger != nil {
		for k, v := range cs.defaultLoggerFields {
			fields[k] = v
		}

		cs.logger.WithFieldsErrorf(fields, format, args...)
	}
}

// Trace logs a message as trace.
func (cs *ChunkSorter) trace(args ...interface{}) {
	if cs.logger != nil {
		cs.logger.WithFieldsTrace(cs.defaultLoggerFields, args...)
	}
}

// Tracef formats a message as trace.
func (cs *ChunkSorter) tracef(format string, args ...interface{}) {
	if cs.logger != nil {
		cs.logger.WithFieldsTracef(cs.defaultLoggerFields, format, args...)
	}
}

// WithFieldsTrace logs a message as trace.
func (cs *ChunkSorter) withFieldsTrace(fields map[string]interface{}, args ...interface{}) {
	if cs.logger != nil {
		for k, v := range cs.defaultLoggerFields {
			fields[k] = v
		}

		cs.logger.WithFieldsTrace(fields, args...)
	}
}

// WithFieldsTracef formats a message as trace.
func (cs *ChunkSorter) withFieldsTracef(fields map[string]interface{}, format string, args ...interface{}) {
	if cs.logger != nil {
		for k, v := range cs.defaultLoggerFields {
			fields[k] = v
		}

		cs.logger.WithFieldsTracef(fields, format, args...)
	}
}
