package logger

import "go.uber.org/zap"

var defaultLogger = zap.Must(zap.NewProduction())

func Debugf(template string, args ...interface{}) {
	defaultLogger.Sugar().Debugf(template, args...)
	return
}

func Infof(template string, args ...interface{}) {
	defaultLogger.Sugar().Infof(template, args...)
	return
}

func Errorf(template string, args ...interface{}) {
	defaultLogger.Sugar().Errorf(template, args...)
	return
}
