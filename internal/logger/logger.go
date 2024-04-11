package logger

import (
	"fmt"
	"os"
	"time"
	"github.com/fatih/color"
	"github.com/lestrrat-go/file-rotatelogs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"git"
)

type LogConfig struct {
	Dev           bool
	NeedFileWrite bool
	LogPath       string
	FilePrefix    string
}

var consoleZapLog *zap.SugaredLogger
var fileZapLog *zap.SugaredLogger

var cfg LogConfig

func GetextendConfig()

func InitLogger(extendConfig LogConfig) {
	cfg = extendConfig
	initConsoleZapLog()
	if cfg.NeedFileWrite {
		initFileZapLogger(cfg.LogPath, cfg.FilePrefix)
	}
}

// initConsoleZapLog
func initConsoleZapLog() {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	// if dev with color
	if cfg.Dev {
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}
	encoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	atom := zap.NewAtomicLevelAt(zap.DebugLevel)
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig), zapcore.AddSync(os.Stdout), atom)
	consoleZapLog = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1), zap.AddStacktrace(zap.ErrorLevel)).Sugar()
}

// initFileZapLogger
func initFileZapLogger(logPath, filename string) {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder

	atom := zap.NewAtomicLevelAt(zap.DebugLevel)
	logf, _ := rotatelogs.New(
		fmt.Sprintf("%s/%s", logPath, filename)+"%Y-%m-%d.log",
		rotatelogs.WithMaxAge(7*24*time.Hour),
		rotatelogs.WithRotationTime(24*time.Hour),
	)
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encoderConfig), zapcore.AddSync(logf), atom)
	fileZapLog = zap.New(core, zap.AddStacktrace(zap.ErrorLevel), zap.AddCaller(), zap.AddCallerSkip(1), zap.AddStacktrace(zap.ErrorLevel)).Sugar()
}

// public func
func Debugf(template string, args ...interface{}) {
	if cfg.Dev {
		consoleZapLog.Debugf(color.MagentaString(template, args...))
	} else {
		consoleZapLog.Debugf(template, args...)
	}

	if cfg.NeedFileWrite {
		fileZapLog.Debugf(template, args...)
	}
}

func Infof(template string, args ...interface{}) {

	consoleZapLog.Infof(color.GreenString(template, args...))

	if cfg.NeedFileWrite {
		fileZapLog.Infof(template, args...)
	}
}

func Warnf(template string, args ...interface{}) {
	if cfg.Dev {
		consoleZapLog.Warnf(color.YellowString(template, args...))
	} else {
		consoleZapLog.Warnf(template, args...)
	}
	if cfg.NeedFileWrite {
		fileZapLog.Warnf(template, args...)
	}
}

func Errorf(template string, args ...interface{}) {
	if cfg.Dev {
		consoleZapLog.Errorf(color.RedString(template, args...))
	} else {
		consoleZapLog.Errorf(template, args...)
	}
	if cfg.NeedFileWrite {
		fileZapLog.Errorf(template, args...)
	}
}
