package httpcontext

import (
	"bytes"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

var authPattern = regexp.MustCompile("^Bearer (.*)$")

var prefPattern = regexp.MustCompile("^([^\\s;]+)(;q=([\\d.]+))?$")

type Response interface {
	Write(w http.ResponseWriter) error
	GetLocation() string
	GetStatus() int
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
	SetContentType(t string)
	Render(resp Response) error
	AccessToken(dst interface{}) error
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

type AccessToken interface {
	Encode() ([]byte, error)
}

type tokenEnvelope struct {
	ExpiresAt time.Time
	Hash      []byte
	Token     AccessToken
}

var secret []byte

func ParseAccessTokens(s []byte, token AccessToken) {
	secret = s
	gob.Register(token)
}

func EncodeToken(token AccessToken, timeout time.Duration) (result string, err error) {
	envelope := &tokenEnvelope{
		ExpiresAt: time.Now().Add(timeout),
		Token:     token,
	}
	h, err := envelope.generateHash()
	if err != nil {
		return
	}
	envelope.Hash = h
	b := &bytes.Buffer{}
	b64Enc := base64.NewEncoder(base64.URLEncoding, b)
	gobEnc := gob.NewEncoder(b64Enc)
	if err = gobEnc.Encode(envelope); err != nil {
		return
	}
	if err = b64Enc.Close(); err != nil {
		return
	}
	result = string(b.Bytes())
	return
}

func (self *tokenEnvelope) generateHash() (result []byte, err error) {
	hash := sha512.New()
	tokenCode, err := self.Token.Encode()
	if err != nil {
		return
	}
	if _, err = hash.Write(tokenCode); err != nil {
		return
	}
	if _, err = hash.Write(secret); err != nil {
		return
	}
	result = hash.Sum(nil)
	return
}

var ErrMissingToken = fmt.Errorf("No authorization header or token query parameter found")

func (self *DefaultHTTPContext) AccessToken(dst interface{}) (err error) {
	for _, authHead := range self.Req().Header["Authorization"] {
		match := authPattern.FindStringSubmatch(authHead)
		if match != nil {
			err = self.parseAccessToken(match[1], dst)
			return
		}
		if authToken := self.Req().URL.Query().Get("token"); authToken != "" {
			err = self.parseAccessToken(authToken, dst)
			return
		}
	}
	err = ErrMissingToken
	return
}

func (self *DefaultHTTPContext) parseAccessToken(d string, dst interface{}) (err error) {
	envelope := &tokenEnvelope{}
	dec := gob.NewDecoder(base64.NewDecoder(base64.URLEncoding, bytes.NewBufferString(d)))
	if err = dec.Decode(&envelope); err != nil {
		err = fmt.Errorf("Invalid AccessToken: %v, %v", d, err)
		return
	}
	if envelope.ExpiresAt.Before(time.Now()) {
		err = fmt.Errorf("Expired AccessToken: %v", envelope)
		return
	}
	wantedHash, err := envelope.generateHash()
	if err != nil {
		return
	}
	if len(wantedHash) != len(envelope.Hash) || subtle.ConstantTimeCompare(envelope.Hash, wantedHash) != 1 {
		err = fmt.Errorf("Invalid AccessToken hash: %v should be %v", hex.EncodeToString(envelope.Hash), hex.EncodeToString(wantedHash))
		return
	}
	dstVal := reflect.ValueOf(dst)
	tokenVal := reflect.ValueOf(envelope.Token)
	if dstVal.Kind() != reflect.Ptr {
		err = fmt.Errorf("%#v is not a pointer", dst)
		return
	}
	if tokenVal.Kind() != reflect.Ptr {
		err = fmt.Errorf("%#v is not a pointer", tokenVal.Interface())
		return
	}
	if dstVal.Type() != tokenVal.Type() {
		err = fmt.Errorf("Can't load a %v into a %v", tokenVal.Type(), dstVal.Type())
		return
	}
	dstVal.Elem().Set(tokenVal.Elem())
	return
}

func (self *DefaultHTTPContext) SetContentType(t string) {
	self.Resp().Header().Set("Content-Type", t)
}

func (self *DefaultHTTPContext) Render(resp Response) error {
	if resp.GetLocation() != "" {
		self.Resp().Header().Set("Location", resp.GetLocation())
	}
	if resp.GetStatus() != 0 {
		self.Resp().WriteHeader(resp.GetStatus())
	}
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
		defer func() {
			if e := recover(); e != nil {
				c.Resp().WriteHeader(500)
				fmt.Fprintf(c.Resp(), "%v\n", e)
				c.Criticalf("%v\n%s", e, debug.Stack())
				panic(e)
			}
		}()
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
