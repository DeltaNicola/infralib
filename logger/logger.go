package logger

import (
	"os"
	"strconv"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.Logger

var encoderConfig = zapcore.EncoderConfig{
	TimeKey:      "timestamp",
	LevelKey:     "level",
	MessageKey:   "message",
	CallerKey:    "caller",
	FunctionKey:  "function",
	EncodeTime:   zapcore.ISO8601TimeEncoder,
	EncodeLevel:  zapcore.CapitalLevelEncoder,
	EncodeCaller: zapcore.ShortCallerEncoder,
}

func InitLogger() {
	var cores []zapcore.Core

	onConsoleEnv, exists := os.LookupEnv("LOG_ON_CONSOLE")
	onConsole, _ := strconv.ParseBool(onConsoleEnv)
	if !exists || onConsole {
		cores = append(cores, consoleLoggerCore())
	}

	onFileEnv, exists := os.LookupEnv("LOG_ON_FILE")
	onFile, _ := strconv.ParseBool(onFileEnv)
	if exists && onFile {
		cores = append(cores, fileLoggerCore())
	}

	onOpenSearchEnv, exists := os.LookupEnv("LOG_ON_OPEN_SEARCH")
	onOpenSearch, _ := strconv.ParseBool(onOpenSearchEnv)
	if exists && onOpenSearch {
		createOpenSearchIndex(os.Getenv("OPEN_SEARCH_ENDPOINT"), os.Getenv("OPEN_SEARCH_INDEX_NAME"))
		cores = append(cores, openSearchLoggercore())
	}

	core := zapcore.NewTee(cores...)

	Logger = zap.New(
		core,
		zap.AddCaller(),
		zap.AddCallerSkip(1),
		zap.AddStacktrace(zap.ErrorLevel),
	)
}

func InitDevLogger() {
	var err error
	Logger, err = zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
}

func Sync() {
	_ = Logger.Sync()
}
