package web

import (
	"fmt"
	"github.com/soundtrackyourbrand/utils/gae"
	"github.com/soundtrackyourbrand/utils/gae/gaecontext"
	"github.com/soundtrackyourbrand/utils/gae/key"
	"github.com/soundtrackyourbrand/utils/gae/memcache"
	"github.com/soundtrackyourbrand/utils/gae/mutex"
	"net/http"
	"reflect"
	"runtime"
	"runtime/debug"
	"time"
)

func testMutex(c gaecontext.HTTPContext) {
	returns := []int{}
	m1 := mutex.New("m1")
	if err := m1.Lock(c, time.Hour); err != nil {
		panic(err)
	}
	defer m1.Unlock(c)
	m2 := mutex.New("m2")
	if err := m2.Lock(c, time.Hour); err != nil {
		panic(err)
	}
	defer m2.Unlock(c)
	m3 := mutex.New("m3")
	if err := m3.Lock(c, time.Hour); err != nil {
		panic(err)
	}
	defer m3.Unlock(c)
	m4 := mutex.New("m4")
	if err := m4.Lock(c, time.Hour); err != nil {
		panic(err)
	}
	defer m4.Unlock(c)
	go func() {
		if err := m1.Lock(c, time.Hour); err != nil {
			panic(err)
		}
		returns = append(returns, 0)
		if err := m2.Unlock(c); err != nil {
			panic(err)
		}
	}()
	go func() {
		if err := m2.Lock(c, time.Hour); err != nil {
			panic(err)
		}
		returns = append(returns, 1)
		if err := m3.Unlock(c); err != nil {
			panic(err)
		}
	}()
	go func() {
		if err := m3.Lock(c, time.Hour); err != nil {
			panic(err)
		}
		returns = append(returns, 2)
		if err := m4.Unlock(c); err != nil {
			panic(err)
		}
	}()
	if err := m1.Unlock(c); err != nil {
		panic(err)
	}
	if err := m4.Lock(c, time.Hour); err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(returns, []int{0, 1, 2}) {
		panic(fmt.Errorf("Wrong order"))
	}
}

type ts struct {
	Id        *key.Key `datastore:"-"`
	Name      string
	Age       int
	Processes []string
}

func (self *ts) Equal(o *ts) bool {
	return self.Id.Equal(o.Id) && self.Name == o.Name && self.Age == o.Age
}

func (self *ts) AfterLoad(c gaecontext.HTTPContext) (result *ts, err error) {
	result = self
	result.Processes = append(result.Processes, "AfterLoad")
	return
}

func (self *ts) BeforeSave(c gaecontext.HTTPContext) (result *ts, err error) {
	result = self
	result.Processes = append(result.Processes, "BeforeSave")
	return
}

func (self *ts) BeforeCreate(c gaecontext.HTTPContext) (result *ts, err error) {
	result = self
	result.Processes = append(result.Processes, "BeforeCreate")
	return
}

func (self *ts) BeforeUpdate(c gaecontext.HTTPContext) (result *ts, err error) {
	result = self
	result.Processes = append(result.Processes, "BeforeUpdate")
	return
}

var findTsByName = gae.Finder(&ts{}, "Name")
var findTsByAncestorAndName = gae.AncestorFinder(&ts{}, "Name")

func testGet(c gaecontext.HTTPContext) {
	t := &ts{
		Id:   key.For(&ts{}, "", 0, nil),
		Name: "the t",
		Age:  12,
	}
	if err := gae.Put(c, t); err != nil {
		panic(err)
	}
	wantedProcesses := []string{"BeforeCreate", "BeforeSave"}
	if !reflect.DeepEqual(t.Processes, wantedProcesses) {
		panic("wrong processes!")
	}
	if t.Id.IntID() == 0 {
		panic("shouldn't be zero")
	}
	t2 := &ts{Id: t.Id}
	if err := gae.GetById(c, t2); err != nil {
		panic(err)
	}
	if !t.Equal(t2) {
		panic("1 should be equal")
	}
	wantedProcesses = append(wantedProcesses, "AfterLoad")
	if !reflect.DeepEqual(t2.Processes, wantedProcesses) {
		panic("wrong processes!")
	}
	t2.Age = 13
	if err := gae.Put(c, t2); err != nil {
		panic(err)
	}
	wantedProcesses = append(wantedProcesses, "BeforeUpdate", "BeforeSave")
	if !reflect.DeepEqual(t2.Processes, wantedProcesses) {
		panic("wrong processes!")
	}
}

func testAncestorFind(c gaecontext.HTTPContext) {
	parentKey := key.New("Parent", "gnu", 0, nil)
	t2 := &ts{
		Id:   key.For(&ts{}, "", 0, parentKey),
		Name: "t again",
		Age:  14,
	}
	if err := gae.Put(c, t2); err != nil {
		panic(err)
	}
	res := []ts{}
	if err := findTsByAncestorAndName(c, &res, parentKey, "t again"); err != nil {
		panic(err)
	}
	if len(res) != 1 {
		panic(fmt.Errorf("wrong number found, wanted 1 but got %+v", res))
	}
	if !(&res[0]).Equal(t2) {
		panic(fmt.Errorf("%+v and %+v should be equal", res[0], t2))
	}
	wantedProcesses := []string{"BeforeCreate", "BeforeSave", "AfterLoad"}
	if !reflect.DeepEqual(wantedProcesses, res[0].Processes) {
		panic("wrong processes")
	}
}

func testFind(c gaecontext.HTTPContext) {
	t2 := &ts{
		Id:   key.For(&ts{}, "", 0, nil),
		Name: "another t",
		Age:  14,
	}
	if err := gae.Put(c, t2); err != nil {
		panic(err)
	}
	time.Sleep(time.Second)
	res := []ts{}
	if err := findTsByName(c, &res, "bla"); err != nil {
		panic(err)
	}
	if len(res) != 0 {
		panic("should be empty")
	}
	if err := findTsByName(c, &res, "another t"); err != nil {
		panic(err)
	}
	if len(res) != 1 {
		panic(fmt.Errorf("wrong number found, wanted 1 but got %+v", res))
	}
	if !(&res[0]).Equal(t2) {
		panic(fmt.Errorf("%+v and %+v should be equal", res[0], t2))
	}
	wantedProcesses := []string{"BeforeCreate", "BeforeSave", "AfterLoad"}
	if !reflect.DeepEqual(wantedProcesses, res[0].Processes) {
		panic("wrong processes")
	}
}

func testMemcacheBasics(c gaecontext.HTTPContext) {
	if err := memcache.Del(c, "s"); err != nil {
		panic(err)
	}
	s := ""
	if _, err := memcache.Get(c, "s", &s); err != nil {
		panic(err)
	}
	if s != "" {
		panic(fmt.Errorf("Wrong value"))
	}
	if success, err := memcache.CAS(c, "s", "x", "y"); err != nil {
		panic(err)
	} else if success {
		panic(fmt.Errorf("Shouldn't succeed"))
	}
	s = "x"
	if err := memcache.Put(c, "s", s); err != nil {
		panic(err)
	}
	s2 := ""
	if _, err := memcache.Get(c, "s", &s2); err != nil {
		panic(err)
	}
	if s2 != "x" {
		panic(fmt.Errorf("Wrong value"))
	}
	if success, err := memcache.CAS(c, "s", "z", "y"); err != nil {
		panic(err)
	} else if success {
		panic(fmt.Errorf("Shouldn't succeed"))
	}
	if success, err := memcache.CAS(c, "s", "x", "y"); err != nil {
		panic(err)
	} else if !success {
		panic(fmt.Errorf("Should have succeeded"))
	}
	s3 := ""
	if _, err := memcache.Get(c, "s", &s3); err != nil {
		panic(err)
	}
	if s3 != "y" {
		panic(fmt.Errorf("Wrong value"))
	}
}

func run(c gaecontext.HTTPContext, f func(c gaecontext.HTTPContext)) {
	defer func() {
		if e := recover(); e != nil {
			msg := fmt.Sprintf("Failed: %v\n%s", e, debug.Stack())
			c.Infof("%v", msg)
			c.Resp().WriteHeader(500)
			fmt.Fprintln(c.Resp(), msg)
		}
	}()
	c.Infof("Running %v", runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name())
	f(c)
	c.Infof("Pass")
}

func test(c gaecontext.HTTPContext) error {
	run(c, testMemcacheBasics)
	run(c, testMutex)
	run(c, testGet)
	run(c, testFind)
	run(c, testAncestorFind)
	return nil
}

func init() {
	http.Handle("/", gaecontext.HTTPHandlerFunc(test))
}
