package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

func fileLoggerCore() zapcore.Core {
	logFilePath, exists := os.LookupEnv("LOG_FILE_PATH")
	if !exists {
		logFilePath = "./app.log"
	}

	lumberjack := &lumberjack.Logger{
		Filename:   logFilePath,
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
