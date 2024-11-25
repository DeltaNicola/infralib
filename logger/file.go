package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

func fileLoggerCore() zapcore.Core {
	lumberjack := &lumberjack.Logger{
		Filename:   "./logs/app.log",
		MaxSize:    10, // In MB
		MaxBackups: 3,
		MaxAge:     28, // In days
		Compress:   true,
	}

	return zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(lumberjack),
		zap.InfoLevel,
	)
}
