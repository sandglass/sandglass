package logy

import "log"
import "os"

//go:generate stringer -type=Level
type Level uint8

const (
	FATAL Level = iota
	INFO
	DEBUG
)

type Logger interface {
	Info(format string, args ...interface{})
	Debug(format string, args ...interface{})
	Fatal(format string, args ...interface{})
	Level() Level
}

type logger struct {
	logger *log.Logger
	level  Level
}

func NewWithLogger(l *log.Logger, level Level) Logger {
	return &logger{
		logger: l,
		level:  level,
	}
}

func NewStdoutLogger(level Level) Logger {
	return NewWithLogger(log.New(os.Stdout, "", log.LstdFlags), level)
}

func (l *logger) log(level Level, format string, args ...interface{}) {
	if l.level < level {
		return
	}
	log := l.logger.Printf
	if level == FATAL {
		log = l.logger.Fatalf
	}
	a := append([]interface{}{level.String()}, args...)
	log("[%s] "+format, a...)
}

func (l *logger) Info(format string, args ...interface{}) {
	l.log(INFO, format, args...)
}

func (l *logger) Debug(format string, args ...interface{}) {
	l.log(DEBUG, format, args...)
}

func (l *logger) Fatal(format string, args ...interface{}) {
	l.log(FATAL, format, args...)
}

func (l *logger) Level() Level {
	return l.level
}

var _ Logger = (*logger)(nil)
