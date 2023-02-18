package logger

import (
	"log"
	"os"
)

// MyLogger implements AppLogger
type MyLogger struct {
	Inf *log.Logger
	Err *log.Logger
}

func New() *MyLogger {
	return &MyLogger{
		Inf: log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime),
		Err: log.New(os.Stderr, "ERROR\t", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

func (l *MyLogger) Info(msg ...string) {
	l.Inf.Println(msg)
}

func (l *MyLogger) Error(msg ...string) {
	l.Err.Println(msg)
}
