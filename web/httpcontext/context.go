package httpcontext

import (
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
)

var prefPattern = regexp.MustCompile("^([^\\s;]+)(;q=([\\d.]+))?$")

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
	SetContentType(t string)
}

type HTTPContextLogger interface {
	HTTPContext
	Logger
}

type DefaultLogger struct {
	debugLogger    *log.Logger
	infoLogger     *log.Logger
	warningLogger  *log.Logger
	errorLogger    *log.Logger
	criticalLogger *log.Logger
}

type DefaultHTTPContext struct {
	Logger
	response http.ResponseWriter
	request  *http.Request
	vars     map[string]string
}

func NewDefaultLogger() *DefaultLogger {
	return &DefaultLogger{
		debugLogger:    log.New(os.Stdout, "DEBUG", 0),
		infoLogger:     log.New(os.Stdout, "INFO", 0),
		warningLogger:  log.New(os.Stdout, "WARNING", 0),
		errorLogger:    log.New(os.Stdout, "ERROR", 0),
		criticalLogger: log.New(os.Stdout, "CRITICAL", 0),
	}
}

func (self *DefaultLogger) Debugf(format string, i ...interface{}) {
	self.debugLogger.Printf(format, i...)
}

func (self *DefaultLogger) Infof(format string, i ...interface{}) {
	self.infoLogger.Printf(format, i...)
}

func (self *DefaultLogger) Warningf(format string, i ...interface{}) {
	self.warningLogger.Printf(format, i...)
}

func (self *DefaultLogger) Errorf(format string, i ...interface{}) {
	self.errorLogger.Printf(format, i...)
}

func (self *DefaultLogger) Criticalf(format string, i ...interface{}) {
	self.criticalLogger.Printf(format, i...)
}

func NewHTTPContext(w http.ResponseWriter, r *http.Request) (result *DefaultHTTPContext) {
	result = &DefaultHTTPContext{
		Logger:   NewDefaultLogger(),
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

func (self *DefaultHTTPContext) SetContentType(t string) {
	self.Resp().Header().Set("Content-Type", t)
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

func HandlerFunc(f func(c HTTPContextLogger)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := NewHTTPContext(w, r)
		defer func() {
			if e := recover(); e != nil {
				c.Resp().WriteHeader(500)
				fmt.Fprintf(c.Resp(), "%v\n", e)
				c.Criticalf("%v\n%s", e, debug.Stack())
				panic(e)
			}
		}()
		f(c)
	})
}
