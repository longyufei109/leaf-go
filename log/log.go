package log

import (
	"fmt"
	stdlog "log"
	"os"
)

var logger = stdlog.New(os.Stderr, "[leaf-go]", stdlog.Ldate|stdlog.Ltime|stdlog.Lshortfile)

func Print(format string, args ...interface{}) {
	_ = logger.Output(3, fmt.Sprintf(format, args...))
}
