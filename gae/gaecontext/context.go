package gaecontext

import (
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/gorilla/mux"
	"github.com/mjibson/appstats"
	"github.com/soundtrackyourbrand/utils"
	"github.com/soundtrackyourbrand/utils/gae"
	"github.com/soundtrackyourbrand/utils/web/httpcontext"
	"github.com/soundtrackyourbrand/utils/web/jsoncontext"

	"appengine"
	"appengine/datastore"
	"appengine/urlfetch"
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
	inTransaction    bool
	afterTransaction []func(GAEContext) error
}

func (self *DefaultContext) AfterTransaction(f interface{}) (err error) {
	// validate that f take one argument of whatever type and returns nothing
	if err = utils.ValidateFuncInput(f, []reflect.Type{
		reflect.TypeOf((*interface{})(nil)).Elem(),
	}); err != nil {
		return
	}
	if err = utils.ValidateFuncOutput(f, []reflect.Type{
		reflect.TypeOf((*error)(nil)).Elem(),
	}); err != nil {
		return
	}
	// validate that whatever argument f took, a GAEContext would implement it
	if !reflect.TypeOf((*GAEContext)(nil)).Elem().AssignableTo(reflect.TypeOf(f).In(0)) {
		err = fmt.Errorf("%v does not take an argument that is satisfied by %v", f, self)
		return
	}
	// create our after func
	afterFunc := func(c GAEContext) (err error) {
		if errVal := reflect.ValueOf(f).Call([]reflect.Value{reflect.ValueOf(c)})[0]; !errVal.IsNil() {
			err = errVal.Interface().(error)
		}
		return
	}
	if self.inTransaction {
		self.afterTransaction = append(self.afterTransaction, afterFunc)
	} else {
		afterFunc(self)
	}
	return
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
	if resp.StatusCode >= 500 {
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

func (self *DefaultContext) Transaction(f interface{}, crossGroup bool) (err error) {
	if self.inTransaction {
		return CallTransactionFunction(self, f)
	}
	var newContext DefaultContext
	/*
	 * Instead of retrying 3 times, something that we see fail multible times, try
	 * get transaction working waiting for max 20 seconds.
	 */
	start := time.Now()
	for time.Since(start) < (time.Second * 20) {
		err = datastore.RunInTransaction(self, func(c appengine.Context) error {
			newContext = *self
			newContext.Context = c
			newContext.inTransaction = true
			return CallTransactionFunction(&newContext, f)
		}, &datastore.TransactionOptions{XG: crossGroup})

		/* Dont fail on concurrent transaction.. Continue trying... */
		if err != datastore.ErrConcurrentTransaction {
			break
		}
	}
	if err != nil {
		return
	}

	// After transaction sucessfull stuff.
	var multiErr appengine.MultiError
	for _, cb := range newContext.afterTransaction {
		if err := cb(self); err != nil {
			multiErr = append(multiErr, err)
		}
	}
	if len(multiErr) > 0 {
		err = multiErr
	}
	return
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

func HTTPHandlerFunc(f func(c HTTPContext) error, scopes ...string) http.Handler {
	return appstats.NewHandler(func(gaeCont appengine.Context, w http.ResponseWriter, r *http.Request) {
		c := NewHTTPContext(gaeCont, httpcontext.NewHTTPContext(w, r))
		httpcontext.Handle(c, func() error {
			return f(c)
		}, scopes...)
	})
}

func JSONHandlerFunc(f func(c JSONContext) (resp jsoncontext.Resp, err error), minAPIVersion int, scopes ...string) http.Handler {
	return appstats.NewHandler(func(gaeCont appengine.Context, w http.ResponseWriter, r *http.Request) {
		c := NewJSONContext(gaeCont, jsoncontext.NewJSONContext(httpcontext.NewHTTPContext(w, r)))
		jsoncontext.Handle(c, func() (jsoncontext.Resp, error) {
			return f(c)
		}, minAPIVersion, scopes...)
	})
}

func DataHandlerFunc(f func(c HTTPContext) (resp *httpcontext.DataResp, err error), scopes ...string) http.Handler {
	return appstats.NewHandler(func(gaeCont appengine.Context, w http.ResponseWriter, r *http.Request) {
		c := NewHTTPContext(gaeCont, httpcontext.NewHTTPContext(w, r))
		httpcontext.DataHandle(c, func() (*httpcontext.DataResp, error) {
			return f(c)
		}, scopes...)
	})
}

func DocHandle(router *mux.Router, f interface{}, path string, method string, minAPIVersion int, scopes ...string) {
	doc, fu := jsoncontext.Document(f, path, method, minAPIVersion, scopes...)
	jsoncontext.Remember(doc)
	router.Path(path).Methods(method).Handler(appstats.NewHandler(func(gaeCont appengine.Context, w http.ResponseWriter, r *http.Request) {
		c := NewJSONContext(gaeCont, jsoncontext.NewJSONContext(httpcontext.NewHTTPContext(w, r)))
		jsoncontext.Handle(c, func() (resp jsoncontext.Resp, err error) {
			return fu(c)
		}, minAPIVersion, scopes...)
	}))
}
