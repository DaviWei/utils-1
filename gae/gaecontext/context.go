package gaecontext

import (
	"appengine"
	"appengine/datastore"
	"appengine/urlfetch"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/mjibson/appstats"
	"github.com/soundtrackyourbrand/utils"
	"github.com/soundtrackyourbrand/utils/gae"
	"github.com/soundtrackyourbrand/utils/web/httpcontext"
	"github.com/soundtrackyourbrand/utils/web/jsoncontext"
	"net/http"
	"reflect"
	"time"
)

type GAEContext interface {
	gae.PersistenceContext
	Transaction(trans interface{}, crossGroup bool) error
	Client() *http.Client
}

type HTTPContext interface {
	GAEContext
	httpcontext.HTTPContext
}

type JSONContext interface {
	GAEContext
	jsoncontext.JSONContext
}

func CallTransactionFunction(c GAEContext, f interface{}) (err error) {
	if err = utils.ValidateFuncInput(f, []reflect.Type{
		reflect.TypeOf((*GAEContext)(nil)).Elem(),
	}); err != nil {
		return
	}
	if err = utils.ValidateFuncOutput(f, []reflect.Type{
		reflect.TypeOf((*error)(nil)).Elem(),
	}); err != nil {
		return
	}
	if errVal := reflect.ValueOf(f).Call([]reflect.Value{reflect.ValueOf(c)})[0]; !errVal.IsNil() {
		err = errVal.Interface().(error)
		return err.(error)
	}
	return nil
}

type DefaultContext struct {
	appengine.Context
	inTransaction bool
}

func (self *DefaultContext) AfterSave(i interface{}) error    { return nil }
func (self *DefaultContext) AfterCreate(i interface{}) error  { return nil }
func (self *DefaultContext) AfterUpdate(i interface{}) error  { return nil }
func (self *DefaultContext) BeforeSave(i interface{}) error   { return nil }
func (self *DefaultContext) AfterLoad(i interface{}) error    { return nil }
func (self *DefaultContext) AfterDelete(i interface{}) error  { return nil }
func (self *DefaultContext) BeforeCreate(i interface{}) error { return nil }
func (self *DefaultContext) BeforeUpdate(i interface{}) error { return nil }

func (self *DefaultContext) Debugf(format string, i ...interface{}) {
	self.Context.Debugf(format, i...)
}

func (self *DefaultContext) Infof(format string, i ...interface{}) {
	self.Context.Infof(format, i...)
}

func (self *DefaultContext) Warningf(format string, i ...interface{}) {
	self.Context.Warningf(format, i...)
}

func (self *DefaultContext) Errorf(format string, i ...interface{}) {
	self.Context.Errorf(format, i...)
}

func (self *DefaultContext) Criticalf(format string, i ...interface{}) {
	self.Context.Criticalf(format, i...)
}

type Transport struct {
	t urlfetch.Transport
}

func (t *Transport) RoundTrip(req *http.Request) (res *http.Response, err error) {
	if t.t.Context.(GAEContext).InTransaction() {
		return nil, fmt.Errorf("Avoid using Client() when in an transaction. %s %s", req.Method, req.URL.String())
	}
	start := time.Now()
	resp, err := t.t.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		t.t.Context.Warningf("Request %s %s %v status %s!\n", req.Method, req.URL.String(), time.Since(start), resp.Status)
	} else if time.Since(start) > (time.Second * 2) {
		t.t.Context.Warningf("Request %s %s took %v to complete %s!\n", req.Method, req.URL.String(), time.Since(start), resp.Status)
	}
	return resp, err
}

func (self *DefaultContext) Client() *http.Client {
	trans := &Transport{}
	trans.t.Context = self
	trans.t.Deadline = time.Second * 30

	return &http.Client{
		Transport: trans,
	}
}

func (self *DefaultContext) InTransaction() bool {
	return self.inTransaction
}

func (self *DefaultContext) Transaction(f interface{}, crossGroup bool) error {
	if self.inTransaction {
		return CallTransactionFunction(self, f)
	}
	return datastore.RunInTransaction(self, func(c appengine.Context) error {
		newContext := *self
		newContext.Context = c
		newContext.inTransaction = true
		return CallTransactionFunction(&newContext, f)
	}, &datastore.TransactionOptions{XG: crossGroup})
}

type DefaultHTTPContext struct {
	GAEContext
	httpcontext.HTTPContext
}

func (self *DefaultHTTPContext) Transaction(f interface{}, crossGroup bool) error {
	return self.GAEContext.Transaction(func(c GAEContext) error {
		newContext := *self
		newContext.GAEContext = c
		return CallTransactionFunction(&newContext, f)
	}, crossGroup)
}

type DefaultJSONContext struct {
	GAEContext
	jsoncontext.JSONContext
}

func (self *DefaultJSONContext) Transaction(f interface{}, crossGroup bool) error {
	return self.GAEContext.Transaction(func(c GAEContext) error {
		newContext := *self
		newContext.GAEContext = c
		return CallTransactionFunction(&newContext, f)
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

func HTTPHandlerFunc(f func(c HTTPContext) error) http.Handler {
	return appstats.NewHandler(func(gaeCont appengine.Context, w http.ResponseWriter, r *http.Request) {
		httpcontext.HandlerFunc(func(httpCont httpcontext.HTTPContextLogger) error {
			c := NewHTTPContext(gaeCont, httpCont)
			return f(c)
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

func DataHandlerFunc(f func(c HTTPContext) (resp *httpcontext.DataResp, err error)) http.Handler {
	return appstats.NewHandler(func(gaeCont appengine.Context, w http.ResponseWriter, r *http.Request) {
		httpcontext.DataHandlerFunc(func(httpCont httpcontext.HTTPContextLogger) (resp *httpcontext.DataResp, err error) {
			c := NewHTTPContext(gaeCont, httpCont)
			return f(c)
		}).ServeHTTP(w, r)
	})
}

func DocHandle(router *mux.Router, f interface{}, path string, methods ...string) {
	if errs := utils.ValidateFuncInputs(f, []reflect.Type{
		reflect.TypeOf((*JSONContext)(nil)).Elem(),
		reflect.TypeOf((*interface{})(nil)).Elem(),
	}, []reflect.Type{
		reflect.TypeOf((*JSONContext)(nil)).Elem(),
	}); len(errs) == 2 {
		panic(fmt.Errorf("%v does not conform. Fix one of %v", errs))
	}
	doc, fu := jsoncontext.Document(f, path, methods...)
	jsoncontext.Remember(doc)
	router.Path(path).Methods(methods...).Handler(appstats.NewHandler(func(gaeCont appengine.Context, w http.ResponseWriter, r *http.Request) {
		jsoncontext.HandlerFunc(func(jsonCont jsoncontext.JSONContextLogger) (resp jsoncontext.Resp, err error) {
			c := NewJSONContext(gaeCont, jsonCont)
			return fu(c)
		}).ServeHTTP(w, r)
	}))
}
