package httpcontext

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/soundtrackyourbrand/utils"
	"io"
	"log"
	"log/syslog"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var ErrMissingToken = fmt.Errorf("No authorization header or token query parameter found")

var authPattern = regexp.MustCompile("^Bearer (.*)$")

var prefPattern = regexp.MustCompile("^([^\\s;]+)(;q=([\\d.]+))?$")

type Response interface {
	Write(w http.ResponseWriter) error
}

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Criticalf(format string, args ...interface{})
}

type HTTPContext interface {
	Vars() map[string]string
	Req() *http.Request
	Resp() http.ResponseWriter
	MostAccepted(name, def string) string
	SetLogger(Logger)
	Render(resp Response) error
	AccessToken(dst interface{}) error
}

type HTTPContextLogger interface {
	HTTPContext
	Logger
}

type DefaultLogger struct {
	DebugLogger    *log.Logger
	InfoLogger     *log.Logger
	WarningLogger  *log.Logger
	ErrorLogger    *log.Logger
	CriticalLogger *log.Logger
}

type DefaultHTTPContext struct {
	Logger
	response http.ResponseWriter
	request  *http.Request
	vars     map[string]string
}

var defaultLogger = NewSTDOUTLogger(4)

func NewDefaultLogger(w io.Writer, level int) (result *DefaultLogger) {
	result = &DefaultLogger{}
	result.CriticalLogger = log.New(w, "CRITICAL: ", 0)
	if level > 0 {
		result.ErrorLogger = log.New(w, "ERROR: ", 0)
	}
	if level > 1 {
		result.WarningLogger = log.New(w, "WARNING: ", 0)
	}
	if level > 2 {
		result.InfoLogger = log.New(w, "INFO: ", 0)
	}
	if level > 3 {
		result.DebugLogger = log.New(w, "DEBUG: ", 0)
	}
	return
}

func NewSTDOUTLogger(level int) (result *DefaultLogger) {
	return NewDefaultLogger(os.Stdout, level)
}

func NewSysLogger(level int) (result *DefaultLogger, err error) {
	result = &DefaultLogger{}
	priorities := []syslog.Priority{syslog.LOG_ERR, syslog.LOG_WARNING, syslog.LOG_INFO, syslog.LOG_DEBUG}
	loggers := []**log.Logger{&result.CriticalLogger, &result.ErrorLogger, &result.WarningLogger, &result.InfoLogger, &result.DebugLogger}
	for index, logger := range loggers {
		if level >= index {
			*logger, err = syslog.NewLogger(priorities[index], 0)
			if err != nil {
				return
			}
		}
	}
	return
}

func (self *DefaultLogger) Debugf(format string, i ...interface{}) {
	if self.DebugLogger != nil {
		self.DebugLogger.Printf(format, i...)
	}
}

func (self *DefaultLogger) Infof(format string, i ...interface{}) {
	if self.InfoLogger != nil {
		self.InfoLogger.Printf(format, i...)
	}
}

func (self *DefaultLogger) Warningf(format string, i ...interface{}) {
	if self.WarningLogger != nil {
		self.WarningLogger.Printf(format, i...)
	}
}

func (self *DefaultLogger) Errorf(format string, i ...interface{}) {
	if self.ErrorLogger != nil {
		self.ErrorLogger.Printf(format, i...)
	}
}

func (self *DefaultLogger) Criticalf(format string, i ...interface{}) {
	self.CriticalLogger.Printf(format, i...)
}

func NewHTTPContext(w http.ResponseWriter, r *http.Request) (result *DefaultHTTPContext) {
	result = &DefaultHTTPContext{
		Logger:   defaultLogger,
		response: w,
		request:  r,
		vars:     mux.Vars(r),
	}
	return
}

func MostAccepted(r *http.Request, name, def string) string {
	bestValue := def
	var bestScore float64 = -1
	var score float64
	for _, pref := range strings.Split(r.Header.Get(name), ",") {
		if match := prefPattern.FindStringSubmatch(pref); match != nil {
			score = 1
			if match[3] != "" {
				score, _ = strconv.ParseFloat(match[3], 64)
			}
			if score > bestScore {
				bestScore = score
				bestValue = match[1]
			}
		}
	}
	return bestValue
}

func (self *DefaultHTTPContext) AccessToken(dst interface{}) (err error) {
	for _, authHead := range self.Req().Header["Authorization"] {
		match := authPattern.FindStringSubmatch(authHead)
		if match != nil {
			err = utils.ParseAccessToken(match[1], dst)
			return
		}
		if authToken := self.Req().URL.Query().Get("token"); authToken != "" {
			err = utils.ParseAccessToken(authToken, dst)
			return
		}
	}
	err = ErrMissingToken
	return
}

func (self *DefaultHTTPContext) Render(resp Response) error {
	return resp.Write(self.Resp())
}

func (self *DefaultHTTPContext) MostAccepted(name, def string) string {
	return MostAccepted(self.Req(), name, def)
}

func (self *DefaultHTTPContext) SetLogger(l Logger) {
	self.Logger = l
}

func (self *DefaultHTTPContext) Req() *http.Request {
	return self.request
}

func (self *DefaultHTTPContext) Resp() http.ResponseWriter {
	return self.response
}

func (self *DefaultHTTPContext) Vars() map[string]string {
	return self.vars
}

func HandlerFunc(f func(c HTTPContextLogger) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := NewHTTPContext(w, r)
		err := f(c)
		if err != nil {
			if errResponse, ok := err.(Response); ok {
				if err2 := c.Render(errResponse); err2 != nil {
					c.Criticalf("Unable to render error %+v: %v", err, err2)
				}
			} else {
				c.Resp().WriteHeader(500)
				fmt.Fprintf(c.Resp(), "%v", err)
			}
			c.Infof("%+v", err)
		}
	})
}
