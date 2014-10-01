package gaecontext

import (
	"fmt"
	"net/http"
	"reflect"
	"time"
	"math/rand"

	"github.com/gorilla/mux"
	"github.com/mjibson/appstats"
	"github.com/soundtrackyourbrand/utils"
	"github.com/soundtrackyourbrand/utils/gae"
	"github.com/soundtrackyourbrand/utils/key"
	"github.com/soundtrackyourbrand/utils/web/httpcontext"
	"github.com/soundtrackyourbrand/utils/web/jsoncontext"

	"appengine"
	"appengine/datastore"
	"strings"
	"appengine/urlfetch"
)

func KindsRenderer(c JSONContext) (status int, result []string, err error) {
	result, err = gae.GetKinds(c)
	return
}

type ServiceStatus struct {
	Status    string        `json:"status"`
	Status5xx string        `json:"status_5xx"`
	Status4xx string        `json:"status_4xx"`
	LogStats  *gae.LogStats `json:"log_stats"`
	Desc      string        `json:"desc"`
}

func ServiceStatusRenderer(ok4xxRatio, ok5xxRatio float64) func(c JSONContext) (status int, result *ServiceStatus, err error) {
	return func(c JSONContext) (status int, result *ServiceStatus, err error) {
		result = &ServiceStatus{
			Desc: "It's Log, Log, it's better than bad, it's good!",
		}
		stats := gae.GetLogStats(c, time.Now().Add(-time.Hour), time.Now(), 128, false)
		result.Status = "status_ok"
		var num4xx float64
		var num5xx float64
		var ratio4xx float64
		var ratio5xx float64
		if stats.Records > 0 {
			for status, num := range stats.Statuses {
				if status >= 400 && status < 500 {
					num4xx += float64(num)
				}
				if status >= 500 && status < 600 {
					num5xx += float64(num)
				}
			}
			ratio4xx = num4xx / float64(stats.Records)
			ratio5xx = num5xx / float64(stats.Records)
		}
		if ratio4xx < ok4xxRatio {
			result.Status4xx = "status_4xx_ok"
		} else {
			result.Status4xx = "status_4xx_bad"
		}
		if ratio5xx < ok5xxRatio {
			result.Status5xx = "status_5xx_ok"
		} else {
			result.Status5xx = "status_5xx_bad"
		}
		result.LogStats = stats
		return
	}
}

type GAEContext interface {
	gae.PersistenceContext
	Transaction(trans interface{}, crossGroup bool) error
	GetAllowHTTPDuringTransactions() bool
	SetAllowHTTPDuringTransactions(b bool)
	Client() *http.Client
	ClientTimeout(time.Duration)
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
		err = utils.NewError(err)
		return
	}
	if err = utils.ValidateFuncOutput(f, []reflect.Type{
		reflect.TypeOf((*error)(nil)).Elem(),
	}); err != nil {
		err = utils.NewError(err)
		return
	}
	if errVal := reflect.ValueOf(f).Call([]reflect.Value{reflect.ValueOf(c)})[0]; !errVal.IsNil() {
		err = utils.NewError(errVal.Interface().(error))
		return
	}
	return nil
}

type DefaultContext struct {
	appengine.Context
	allowHTTPDuringTransactions bool
	inTransaction               bool
	afterTransaction            []func(GAEContext) error
	clientTimeout               time.Duration
}

func (self *DefaultContext) GetAllowHTTPDuringTransactions() bool {
	return self.allowHTTPDuringTransactions
}

func (self *DefaultContext) SetAllowHTTPDuringTransactions(b bool) {
	self.allowHTTPDuringTransactions = b
}

func (self *DefaultContext) ClientTimeout(d time.Duration) {
	self.clientTimeout = d
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
		if err = afterFunc(self); err != nil {
			return
		}
	}
	return
}

func (self *DefaultContext) AfterSave(i interface{}) error    { return nil }
func (self *DefaultContext) AfterCreate(i interface{}) error  { return nil }
func (self *DefaultContext) AfterUpdate(i interface{}) error  { return nil }
func (self *DefaultContext) BeforeSave(i interface{}) error   { return nil }
func (self *DefaultContext) AfterLoad(i interface{}) error    { return nil }
func (self *DefaultContext) AfterDelete(i interface{}) error  { return nil }
func (self *DefaultContext) BeforeDelete(i interface{}) error { return nil }
func (self *DefaultContext) BeforeCreate(i interface{}) error { return nil }
func (self *DefaultContext) BeforeUpdate(i interface{}) error { return nil }

func (self *DefaultContext) Debugf(format string, i ...interface{}) {
	self.Context.Debugf(format, i...)
}

func (self *DefaultContext) split(format string, i ...interface{}) (result []string) {
	msg := fmt.Sprintf(format, i...)
	for len(msg) > 8000 {
		result = append(result, msg[:8000])
		msg = msg[8000:]
	}
	result = append(result, msg)
	return
}

func (self *DefaultContext) Infof(format string, i ...interface{}) {
	for _, m := range self.split(format, i...) {
		self.Context.Infof("%v", m)
	}
}

func (self *DefaultContext) Warningf(format string, i ...interface{}) {
	for _, m := range self.split(format, i...) {
		self.Context.Warningf("%v", m)
	}
}

func (self *DefaultContext) Errorf(format string, i ...interface{}) {
	for _, m := range self.split(format, i...) {
		self.Context.Errorf("%v", m)
	}
}

func (self *DefaultContext) Criticalf(format string, i ...interface{}) {
	for _, m := range self.split(format, i...) {
		self.Context.Criticalf("%v", m)
	}
}

type Transport struct {
	T      urlfetch.Transport
	Header http.Header
}

func (t *Transport) RoundTrip(req *http.Request) (res *http.Response, err error) {
	cont := t.T.Context.(GAEContext)
	if cont.InTransaction() && !cont.GetAllowHTTPDuringTransactions() {
		return nil, fmt.Errorf("Avoid using Client() when in an transaction. %s %s", req.Method, req.URL.String())
	}
	for key, values := range t.Header {
		req.Header[key] = values
	}
	start := time.Now()
	curly := utils.ToCurl(req)
	resp, err := t.T.RoundTrip(req)
	if err != nil {
		t.T.Context.Warningf("Error doing roundtrip for %+v: %v\n%v\nCURL to replicate:\n%v", req, resp, err, curly)
		return nil, err
	}
	if resp.StatusCode >= 500 {
		t.T.Context.Warningf("5xx doing roundtrip for %+v: %v\nCURL to replicate:\n%v", req, resp, curly)
	} else if time.Since(start) > (time.Second * 2) {
		t.T.Context.Warningf("Slow response doing roundtrip for %+v: %v\nCURL to replicate:\n%v", req, resp, curly)
	}
	return resp, err
}

func (self *DefaultContext) Client() *http.Client {
	trans := &Transport{
		Header: http.Header{},
	}
	trans.T.Context = self
	if self.clientTimeout == 0 {
		trans.T.Deadline = time.Second * 30
	} else {
		trans.T.Deadline = self.clientTimeout
	}

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
	tries := 0
	for time.Since(start) < (time.Second * 20) {
		hasConcErr := false
		err = datastore.RunInTransaction(self, func(c appengine.Context) error {
			newContext = *self
			newContext.Context = c
			newContext.inTransaction = true
			return CallTransactionFunction(&newContext, f)
		}, &datastore.TransactionOptions{XG: crossGroup})
		if err == nil {
			break
		}
		/* Dont fail on concurrent transaction.. Continue trying... */
		if dserr, ok := err.(utils.DefaultStackError); ok {
			// our own stack errors, based on a concurrent transaction error
			if dserr.Source == datastore.ErrConcurrentTransaction {
				hasConcErr = true
			} else {
				// if they are based on appengine or utils multierrors, check for concurrency errors inside
				if merr, ok := dserr.Source.(appengine.MultiError); ok {
					for _, e := range merr {
						if e == datastore.ErrConcurrentTransaction {
							hasConcErr = true
							break
						}
					}
				} else if merr, ok := dserr.Source.(utils.MultiError); ok {
					for _, e := range merr {
						if e == datastore.ErrConcurrentTransaction {
							hasConcErr = true
							break
						}
					}
				}
			}
		} else if err == datastore.ErrConcurrentTransaction {
			// or if they ARE concurrency errors!
			hasConcErr = true
		}
		if !hasConcErr && strings.Contains(strings.ToLower(err.Error()), "concurrent") {
			// or, if they are the special black ops concurrency errors that google never talk openly about
			hasConcErr = true
		}
		if !hasConcErr && strings.Contains(strings.ToLower(err.Error()), "transaction closed") {
			// or, they are the even more magical "transaction closed" errors that don't even know about the cause why it was closed
			hasConcErr = true
		}
		if hasConcErr {
			self.Debugf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! DANGER ! Failed to run %v in transaction due to %v, retrying... !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", f, err)
			tries+= 1
			time.Sleep(time.Millisecond * time.Duration(rand.Int63() % int64(500* tries)))
		} else {
			self.Debugf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! DANGER ! Failed to run %v in transaction due to %#v BAMDFFF !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  %v", f, err, err.Error())
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

func JSONHandlerFunc(f func(c JSONContext) (resp jsoncontext.Resp, err error), minAPIVersion, maxAPIVersion int, scopes ...string) http.Handler {
	return appstats.NewHandler(func(gaeCont appengine.Context, w http.ResponseWriter, r *http.Request) {
		c := NewJSONContext(gaeCont, jsoncontext.NewJSONContext(httpcontext.NewHTTPContext(w, r)))
		jsoncontext.Handle(c, func() (jsoncontext.Resp, error) {
			return f(c)
		}, minAPIVersion, maxAPIVersion, scopes...)
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

func DocHandle(router *mux.Router, f interface{}, path string, method string, minAPIVersion, maxAPIVersion int, scopes ...string) {
	doc, fu := jsoncontext.Document(f, path, method, minAPIVersion, maxAPIVersion, scopes...)
	jsoncontext.Remember(doc)
	router.Path(path).Methods(method).Handler(appstats.NewHandler(func(gaeCont appengine.Context, w http.ResponseWriter, r *http.Request) {
		c := NewJSONContext(gaeCont, jsoncontext.NewJSONContext(httpcontext.NewHTTPContext(w, r)))
		jsoncontext.Handle(c, func() (resp jsoncontext.Resp, err error) {
			return fu(c)
		}, minAPIVersion, maxAPIVersion, scopes...)
	}))
}

type KeyLock struct {
	Id     key.Key `datastore:"-"`
	Entity key.Key
}

type ErrLockTaken struct {
	Key    key.Key
	Entity key.Key
	Stack  string
}

func (self ErrLockTaken) GetStack() string {
	return self.Stack
}

func (self ErrLockTaken) Error() string {
	return fmt.Sprintf("%v is already taken by %v", self.Key, self.Entity)
}

func (self *KeyLock) LockedBy(c GAEContext) (isLocked bool, lockedBy key.Key, err error) {
	existingLock := &KeyLock{Id: self.Id}
	if err = gae.GetById(c, existingLock); err != nil {
		if _, ok := err.(gae.ErrNoSuchEntity); ok {
			err = nil
			return
		} else {
			return
		}
	}
	isLocked = true
	lockedBy = existingLock.Entity
	return
}

func (self *KeyLock) Lock(c GAEContext) error {
	snapshot := *self
	return c.Transaction(func(c GAEContext) (err error) {
		*self = snapshot
		existingLock := &KeyLock{Id: self.Id}
		err = gae.GetById(c, existingLock)
		if _, ok := err.(gae.ErrNoSuchEntity); ok {
			err = nil
		} else if err == nil {
			err = ErrLockTaken{
				Key:    self.Id,
				Entity: existingLock.Entity,
				Stack:  utils.Stack(),
			}
		}
		if err != nil {
			return
		}
		err = gae.Put(c, self)
		return
	}, false)
}

func (self *KeyLock) Unlock(c GAEContext) (err error) {
	snapshot := *self
	return c.Transaction(func(c GAEContext) (err error) {
		*self = snapshot
		existingLock := &KeyLock{Id: self.Id}
		if err = gae.GetById(c, existingLock); err != nil {
			return
		}
		if existingLock.Entity != self.Entity {
			err = utils.Errorf("%+v doesn't own %v", self, self.Entity)
			return
		}
		err = gae.Del(c, existingLock)
		return
	}, false)
}

type Counter struct {
	Count int64
}

const (
	GAEContextCounterKind = "GAEContextCounterKind"
)

/*
AcquireSequenceNo will return the next number in the named sequence.
*/
func AcquireSequenceNo(c GAEContext, name string) (result int64, err error) {
	result = 0
	key := datastore.NewKey(c, GAEContextCounterKind, name, 0, nil)
	for {
		err = c.Transaction(func(c GAEContext) (err error) {

			var x Counter
			if err = datastore.Get(c, key, &x); err != nil && err != datastore.ErrNoSuchEntity {
				return
			}
			x.Count++
			if _, err = datastore.Put(c, key, &x); err != nil {
				return
			}
			result = x.Count
			return

		}, false)

		/* Dont fail on concurrent transaction.. Continue trying... */
		if err != datastore.ErrConcurrentTransaction {
			break
		}
	}
	return
}
