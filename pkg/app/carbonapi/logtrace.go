package carbonapi

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func Trace(lg *zap.Logger, msg string, fields ...zapcore.Field) {
	if l := lg.Check(zapcore.DebugLevel, msg); l != nil {
		l.Write(fields...)
	}
}
