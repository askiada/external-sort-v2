package chunksmerger

// Debug logs a message as debug.
func (cm *ChunksMerger) debug(args ...interface{}) {
	if cm.logger != nil {
		cm.logger.Debug(args...)
	}
}

func (cm *ChunksMerger) debugf(format string, args ...interface{}) {
	if cm.logger != nil {
		cm.logger.WithFieldsDebugf(cm.defaultLoggerFields, format, args...)
	}
}

// WithFieldsDebug logs a message as debug.
func (cm *ChunksMerger) withFieldsDebug(fields map[string]interface{}, args ...interface{}) {
	if cm.logger != nil {
		for k, v := range cm.defaultLoggerFields {
			fields[k] = v
		}

		cm.logger.WithFieldsDebug(fields, args...)
	}
}

// WithFieldsDebugf formats a message as debug.
func (cm *ChunksMerger) withFieldsDebugf(fields map[string]interface{}, format string, args ...interface{}) {
	if cm.logger != nil {
		for k, v := range cm.defaultLoggerFields {
			fields[k] = v
		}

		cm.logger.WithFieldsDebugf(fields, format, args...)
	}
}

// Info logs a message as info.
func (cm *ChunksMerger) info(args ...interface{}) {
	if cm.logger != nil {
		cm.logger.WithFieldsInfo(cm.defaultLoggerFields, args...)
	}
}

// Infof formats a message as info.
func (cm *ChunksMerger) infof(format string, args ...interface{}) {
	if cm.logger != nil {
		cm.logger.WithFieldsInfof(cm.defaultLoggerFields, format, args...)
	}
}

// WithFieldsInfo logs a message as info.
func (cm *ChunksMerger) withFieldsInfo(fields map[string]interface{}, args ...interface{}) {
	if cm.logger != nil {
		for k, v := range cm.defaultLoggerFields {
			fields[k] = v
		}

		cm.logger.WithFieldsInfo(fields, args...)
	}
}

// WithFieldsInfof formats a message as info.
func (cm *ChunksMerger) withFieldsInfof(fields map[string]interface{}, format string, args ...interface{}) {
	if cm.logger != nil {
		for k, v := range cm.defaultLoggerFields {
			fields[k] = v
		}

		cm.logger.WithFieldsInfof(fields, format, args...)
	}
}

// Warn logs a message as warn.
func (cm *ChunksMerger) warn(args ...interface{}) {
	if cm.logger != nil {
		cm.logger.WithFieldsWarn(cm.defaultLoggerFields, args...)
	}
}

// Warnf formats a message as warn.
func (cm *ChunksMerger) warnf(format string, args ...interface{}) {
	if cm.logger != nil {
		cm.logger.WithFieldsWarnf(cm.defaultLoggerFields, format, args...)
	}
}

// WithFieldsWarn logs a message as warn.
func (cm *ChunksMerger) withFieldsWarn(fields map[string]interface{}, args ...interface{}) {
	if cm.logger != nil {
		for k, v := range cm.defaultLoggerFields {
			fields[k] = v
		}

		cm.logger.WithFieldsWarn(fields, args...)
	}
}

// WithFieldsWarnf formats a message as warn.
func (cm *ChunksMerger) withFieldsWarnf(fields map[string]interface{}, format string, args ...interface{}) {
	if cm.logger != nil {
		for k, v := range cm.defaultLoggerFields {
			fields[k] = v
		}

		cm.logger.WithFieldsWarnf(fields, format, args...)
	}
}

// Error logs a message as error.
func (cm *ChunksMerger) error(args ...interface{}) {
	if cm.logger != nil {
		cm.logger.WithFieldsError(cm.defaultLoggerFields, args...)
	}
}

// Errorf formats a message as error.
func (cm *ChunksMerger) errorf(format string, args ...interface{}) {
	if cm.logger != nil {
		cm.logger.WithFieldsErrorf(cm.defaultLoggerFields, format, args...)
	}
}

// WithFieldsError logs a message as error.
func (cm *ChunksMerger) withFieldsError(fields map[string]interface{}, args ...interface{}) {
	if cm.logger != nil {
		for k, v := range cm.defaultLoggerFields {
			fields[k] = v
		}

		cm.logger.WithFieldsError(fields, args...)
	}
}

// WithFieldsErrorf formats a message as error.
func (cm *ChunksMerger) withFieldsErrorf(fields map[string]interface{}, format string, args ...interface{}) {
	if cm.logger != nil {
		for k, v := range cm.defaultLoggerFields {
			fields[k] = v
		}

		cm.logger.WithFieldsErrorf(fields, format, args...)
	}
}

// Trace logs a message as trace.
func (cm *ChunksMerger) trace(args ...interface{}) {
	if cm.logger != nil {
		cm.logger.WithFieldsTrace(cm.defaultLoggerFields, args...)
	}
}

// Tracef formats a message as trace.
func (cm *ChunksMerger) tracef(format string, args ...interface{}) {
	if cm.logger != nil {
		cm.logger.WithFieldsTracef(cm.defaultLoggerFields, format, args...)
	}
}

// WithFieldsTrace logs a message as trace.
func (cm *ChunksMerger) withFieldsTrace(fields map[string]interface{}, args ...interface{}) {
	if cm.logger != nil {
		for k, v := range cm.defaultLoggerFields {
			fields[k] = v
		}

		cm.logger.WithFieldsTrace(fields, args...)
	}
}

// WithFieldsTracef formats a message as trace.
func (cm *ChunksMerger) withFieldsTracef(fields map[string]interface{}, format string, args ...interface{}) {
	if cm.logger != nil {
		for k, v := range cm.defaultLoggerFields {
			fields[k] = v
		}

		cm.logger.WithFieldsTracef(fields, format, args...)
	}
}
