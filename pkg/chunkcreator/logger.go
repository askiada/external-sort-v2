package chunkcreator

// Debug logs a message as debug.
func (cc *ChunkCreator) debug(args ...interface{}) {
	if cc.logger != nil {
		cc.logger.Debug(args...)
	}
}

func (cc *ChunkCreator) debugf(format string, args ...interface{}) {
	if cc.logger != nil {
		cc.logger.WithFieldsDebugf(cc.defaultLoggerFields, format, args...)
	}
}

// WithFieldsDebug logs a message as debug.
func (cc *ChunkCreator) withFieldsDebug(fields map[string]interface{}, args ...interface{}) {
	if cc.logger != nil {
		for k, v := range cc.defaultLoggerFields {
			fields[k] = v
		}

		cc.logger.WithFieldsDebug(fields, args...)
	}
}

// WithFieldsDebugf formats a message as debug.
func (cc *ChunkCreator) withFieldsDebugf(fields map[string]interface{}, format string, args ...interface{}) {
	if cc.logger != nil {
		for k, v := range cc.defaultLoggerFields {
			fields[k] = v
		}

		cc.logger.WithFieldsDebugf(fields, format, args...)
	}
}

// Info logs a message as info.
func (cc *ChunkCreator) info(args ...interface{}) {
	if cc.logger != nil {
		cc.logger.WithFieldsInfo(cc.defaultLoggerFields, args...)
	}
}

// Infof formats a message as info.
func (cc *ChunkCreator) infof(format string, args ...interface{}) {
	if cc.logger != nil {
		cc.logger.WithFieldsInfof(cc.defaultLoggerFields, format, args...)
	}
}

// WithFieldsInfo logs a message as info.
func (cc *ChunkCreator) withFieldsInfo(fields map[string]interface{}, args ...interface{}) {
	if cc.logger != nil {
		for k, v := range cc.defaultLoggerFields {
			fields[k] = v
		}

		cc.logger.WithFieldsInfo(fields, args...)
	}
}

// WithFieldsInfof formats a message as info.
func (cc *ChunkCreator) withFieldsInfof(fields map[string]interface{}, format string, args ...interface{}) {
	if cc.logger != nil {
		for k, v := range cc.defaultLoggerFields {
			fields[k] = v
		}

		cc.logger.WithFieldsInfof(fields, format, args...)
	}
}

// Warn logs a message as warn.
func (cc *ChunkCreator) warn(args ...interface{}) {
	if cc.logger != nil {
		cc.logger.WithFieldsWarn(cc.defaultLoggerFields, args...)
	}
}

// Warnf formats a message as warn.
func (cc *ChunkCreator) warnf(format string, args ...interface{}) {
	if cc.logger != nil {
		cc.logger.WithFieldsWarnf(cc.defaultLoggerFields, format, args...)
	}
}

// WithFieldsWarn logs a message as warn.
func (cc *ChunkCreator) withFieldsWarn(fields map[string]interface{}, args ...interface{}) {
	if cc.logger != nil {
		for k, v := range cc.defaultLoggerFields {
			fields[k] = v
		}

		cc.logger.WithFieldsWarn(fields, args...)
	}
}

// WithFieldsWarnf formats a message as warn.
func (cc *ChunkCreator) withFieldsWarnf(fields map[string]interface{}, format string, args ...interface{}) {
	if cc.logger != nil {
		for k, v := range cc.defaultLoggerFields {
			fields[k] = v
		}

		cc.logger.WithFieldsWarnf(fields, format, args...)
	}
}

// Error logs a message as error.
func (cc *ChunkCreator) error(args ...interface{}) {
	if cc.logger != nil {
		cc.logger.WithFieldsError(cc.defaultLoggerFields, args...)
	}
}

// Errorf formats a message as error.
func (cc *ChunkCreator) errorf(format string, args ...interface{}) {
	if cc.logger != nil {
		cc.logger.WithFieldsErrorf(cc.defaultLoggerFields, format, args...)
	}
}

// WithFieldsError logs a message as error.
func (cc *ChunkCreator) withFieldsError(fields map[string]interface{}, args ...interface{}) {
	if cc.logger != nil {
		for k, v := range cc.defaultLoggerFields {
			fields[k] = v
		}

		cc.logger.WithFieldsError(fields, args...)
	}
}

// WithFieldsErrorf formats a message as error.
func (cc *ChunkCreator) withFieldsErrorf(fields map[string]interface{}, format string, args ...interface{}) {
	if cc.logger != nil {
		for k, v := range cc.defaultLoggerFields {
			fields[k] = v
		}

		cc.logger.WithFieldsErrorf(fields, format, args...)
	}
}

// Trace logs a message as trace.
func (cc *ChunkCreator) trace(args ...interface{}) {
	if cc.logger != nil {
		cc.logger.WithFieldsTrace(cc.defaultLoggerFields, args...)
	}
}

// Tracef formats a message as trace.
func (cc *ChunkCreator) tracef(format string, args ...interface{}) {
	if cc.logger != nil {
		cc.logger.WithFieldsTracef(cc.defaultLoggerFields, format, args...)
	}
}

// WithFieldsTrace logs a message as trace.
func (cc *ChunkCreator) withFieldsTrace(fields map[string]interface{}, args ...interface{}) {
	if cc.logger != nil {
		for k, v := range cc.defaultLoggerFields {
			fields[k] = v
		}

		cc.logger.WithFieldsTrace(fields, args...)
	}
}

// WithFieldsTracef formats a message as trace.
func (cc *ChunkCreator) withFieldsTracef(fields map[string]interface{}, format string, args ...interface{}) {
	if cc.logger != nil {
		for k, v := range cc.defaultLoggerFields {
			fields[k] = v
		}

		cc.logger.WithFieldsTracef(fields, format, args...)
	}
}
