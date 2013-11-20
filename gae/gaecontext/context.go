package gaecontext

import (
	"appengine"
	"appengine/datastore"
	"appengine/urlfetch"
	"fmt"
	"github.com/mjibson/appstats"
	"github.com/soundtrackyourbrand/utils/web/httpcontext"
	"github.com/soundtrackyourbrand/utils/web/jsoncontext"
	"net/http"
	"reflect"
)

type GAEContext interface {
	appengine.Context
	InTransaction() bool
	Transaction(trans interface{}, crossGroup bool) error
	Client() *http.Client
}

func CallTransactionFunction(c GAEContext, f interface{}) error {
	val := reflect.ValueOf(f)
	if val.Kind() != reflect.Func {
		return fmt.Errorf("%v is not a function", f)
	}
	typ := val.Type()
	if typ.NumOut() != 1 {
		return fmt.Errorf("%v does not return exactly one value", f)
	}
	if !typ.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return fmt.Errorf("%v does not return an error", f)
	}
	if typ.NumIn() != 1 {
		return fmt.Errorf("%v does not take exactly one argument", f)
	}
	argValue := reflect.ValueOf(c)
	if !argValue.Type().AssignableTo(typ.In(0)) {
		return fmt.Errorf("%v does not take exactly one argument assignable to GAEContext", f)
	}
	if err := val.Call([]reflect.Value{argValue})[0].Interface(); err != nil {
		return err.(error)
	}
	return nil
}

func (self DefaultContext) Debugf(format string, i ...interface{}) {
	self.Context.Debugf(format, i...)
}

func (self DefaultContext) Infof(format string, i ...interface{}) {
	self.Context.Infof(format, i...)
}

func (self DefaultContext) Warningf(format string, i ...interface{}) {
	self.Context.Warningf(format, i...)
}

func (self DefaultContext) Errorf(format string, i ...interface{}) {
	self.Context.Errorf(format, i...)
}

func (self DefaultContext) Criticalf(format string, i ...interface{}) {
	self.Context.Criticalf(format, i...)
}

func (self DefaultContext) Client() *http.Client {
	return urlfetch.Client(self)
}

func (self DefaultContext) InTransaction() bool {
	return self.inTransaction
}

type DefaultContext struct {
	appengine.Context
	inTransaction bool
}

func (self DefaultContext) Transaction(f interface{}, crossGroup bool) error {
	if self.inTransaction {
		return CallTransactionFunction(self, f)
	}
	return datastore.RunInTransaction(self, func(c appengine.Context) error {
		newContext := self
		newContext.Context = c
		newContext.inTransaction = true
		return CallTransactionFunction(self, f)
	}, &datastore.TransactionOptions{XG: crossGroup})
}

type HTTPContext interface {
	GAEContext
	httpcontext.HTTPContext
}

type JSONContext interface {
	GAEContext
	jsoncontext.JSONContext
}

type DefaultHTTPContext struct {
	GAEContext
	httpcontext.HTTPContext
}

func (self DefaultHTTPContext) Transaction(f interface{}, crossGroup bool) error {
	return self.GAEContext.Transaction(func(c GAEContext) error {
		newContext := self
		newContext.GAEContext = c
		return CallTransactionFunction(newContext, f)
	}, crossGroup)
}

type DefaultJSONContext struct {
	GAEContext
	jsoncontext.JSONContext
}

func (self DefaultJSONContext) Transaction(f interface{}, crossGroup bool) error {
	return self.GAEContext.Transaction(func(c GAEContext) error {
		newContext := self
		newContext.GAEContext = c
		return CallTransactionFunction(newContext, f)
	}, crossGroup)
}

func NewContext(gaeCont appengine.Context) (result *DefaultContext) {
	return &DefaultContext{
		Context: gaeCont,
	}
}

func NewHTTPContext(gaeCont appengine.Context, httpCont httpcontext.HTTPContextLogger) (result *DefaultHTTPContext) {
	result = &DefaultHTTPContext{
		GAEContext:  NewContext(gaeCont),
		HTTPContext: httpCont,
	}
	result.SetLogger(gaeCont)
	return
}

func NewJSONContext(gaeCont appengine.Context, jsonCont jsoncontext.JSONContextLogger) (result *DefaultJSONContext) {
	result = &DefaultJSONContext{
		GAEContext:  NewContext(gaeCont),
		JSONContext: jsonCont,
	}
	result.SetLogger(gaeCont)
	return
}

func HTTPHandlerFunc(f func(c HTTPContext)) http.Handler {
	return appstats.NewHandler(func(gaeCont appengine.Context, w http.ResponseWriter, r *http.Request) {
		httpcontext.HandlerFunc(func(httpCont httpcontext.HTTPContextLogger) {
			c := NewHTTPContext(gaeCont, httpCont)
			f(c)
		}).ServeHTTP(w, r)
	})
}

func JSONHandlerFunc(f func(c JSONContext) (resp jsoncontext.Resp, err error)) http.Handler {
	return appstats.NewHandler(func(gaeCont appengine.Context, w http.ResponseWriter, r *http.Request) {
		jsoncontext.HandlerFunc(func(jsonCont jsoncontext.JSONContextLogger) (resp jsoncontext.Resp, err error) {
			c := NewJSONContext(gaeCont, jsonCont)
			return f(c)
		}).ServeHTTP(w, r)
	})
}
