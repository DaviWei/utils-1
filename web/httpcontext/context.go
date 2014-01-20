package httpcontext

import (
	"fmt"
	"io"
	"log"
	"log/syslog"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"runtime"
	"strings"
	"github.com/gorilla/mux"
	"github.com/soundtrackyourbrand/utils"
)

var ErrMissingToken = fmt.Errorf("No authorization header or token query parameter found")

var authPattern = regexp.MustCompile("^Bearer (.*)$")

var prefPattern = regexp.MustCompile("^([^\\s;]+)(;q=([\\d.]+))?$")

type Error struct {
	Status int
	Body   interface{}
	Cause  error
	Info   string
	Stack  []byte
}

func (self Error) String() string {
	return fmt.Sprintf("Status: %v\nBody: %v\nCause: %v\nInfo: %v\nStack: %s", self.Status, self.Body, self.Cause, self.Info, self.Stack)
}

func NewError(status int, body interface{}, info string, cause error) Error {
	err := Error{
		Status: status,
		Body:   body,
		Cause:  cause,
		Info:   info,
	}

	err.Stack = make([]byte, 1024*1024)
	runtime.Stack(err.Stack, true)
	return err
}

func (self Error) Respond(c HTTPContextLogger) (err error) {
	c.Infof("ERROR httpcontext %v", self.Status)
	if self.Status != 0 {
		c.Resp().WriteHeader(self.Status)
	}
	if self.Body != nil {
		_, err = fmt.Fprint(c.Resp(), self.Body)
	}
	return
}

func (self Error) Error() string {
	return fmt.Sprintf("%v, %+v, %v, %#v", self.Status, self.Body, self.Cause, self.Info)
}

type Responder interface {
	Respond(c HTTPContextLogger) error
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
	AccessToken(dst utils.AccessToken) (utils.AccessToken, error)
	CheckScopes([]string) error
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
	priorities := []syslog.Priority{syslog.LOG_CRIT, syslog.LOG_ERR, syslog.LOG_WARNING, syslog.LOG_INFO, syslog.LOG_DEBUG}
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

func (self *DefaultHTTPContext) AccessToken(dst utils.AccessToken) (result utils.AccessToken, err error) {
	if self.Req() == nil {
		err = ErrMissingToken
		return
	}
	for _, authHead := range self.Req().Header["Authorization"] {
		match := authPattern.FindStringSubmatch(authHead)
		if match != nil {
			result, err = utils.ParseAccessToken(match[1], dst)
			return
		}
		if authToken := self.Req().URL.Query().Get("token"); authToken != "" {
			result, err = utils.ParseAccessToken(authToken, dst)
			return
		}
	}
	err = ErrMissingToken
	return
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

func (self *DefaultHTTPContext) CheckScopes(allowedScopes []string) (err error) {
	if len(allowedScopes) == 0 {
		return
	}
	token, err := self.AccessToken(nil)
	if err != nil {
		err = NewError(401, "Unauthorized", "", err)
		return
	}
	for _, allowedScope := range allowedScopes {
		for _, scope := range token.Scopes() {
			if scope == allowedScope {
				return
			}
		}
	}
	return NewError(401, "Unauthorized", fmt.Sprintf("Requires one of %+v, but got %+v", allowedScopes, token.Scopes()), nil)
}

func Handle(c HTTPContextLogger, f func() error, scopes ...string) {
	err := c.CheckScopes(scopes)
	if err == nil {
		err = f()
	}
	if err != nil {
		if errResponse, ok := err.(Responder); ok {
			if err2 := errResponse.Respond(c); err2 != nil {
				c.Resp().WriteHeader(500)
				fmt.Fprintf(c.Resp(), "Unable to render the proper error %+v: %v", err, err2)
			}
		} else {
			c.Resp().WriteHeader(500)
			fmt.Fprintf(c.Resp(), "%v", err)
		}
		if er, ok := err.(Error); ok {
			c.Errorf("%v\n%s\n\n", c.Req().URL, er.String())
		} else {
			c.Errorf("%v\n\t%+v\n\n", c.Req().URL, err)
		}

	}
}

func HandlerFunc(f func(c HTTPContextLogger) error, scopes ...string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := NewHTTPContext(w, r)
		Handle(c, func() error {
			return f(c)
		}, scopes...)
	})
}
