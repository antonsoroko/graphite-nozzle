package logger

import (
	"io"
	"log"
	"os"
)

var (
	Trace   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
	Debug   *log.Logger
)

func InitLogHandlers(
	traceHandle io.Writer,
	infoHandle io.Writer,
	warningHandle io.Writer,
	errorHandle io.Writer,
	debugHandle io.Writer) {

	Trace = log.New(traceHandle,
		"TRACE: ",
		log.LstdFlags|log.Lshortfile)

	Info = log.New(infoHandle,
		"INFO: ",
		log.LstdFlags)

	Warning = log.New(warningHandle,
		"WARNING: ",
		log.LstdFlags)

	Error = log.New(errorHandle,
		"ERROR: ",
		log.LstdFlags)

	Debug = log.New(debugHandle,
		"DEBUG: ",
		log.LstdFlags)
}

func init() {
	InitLogHandlers(os.Stdout, os.Stdout, os.Stdout, os.Stderr, os.Stdout)
	Info.Println("Logger was initialized")
}
