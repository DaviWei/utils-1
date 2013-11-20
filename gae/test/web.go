package web

import (
	"fmt"
	"github.com/soundtrackyourbrand/utils/gae/gaecontext"
	"github.com/soundtrackyourbrand/utils/gae/memcache"
	"github.com/soundtrackyourbrand/utils/gae/mutex"
	"net/http"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
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
			msg := fmt.Sprintf("%v\nFailed: %v", strings.Split(string(debug.Stack()), "\n")[3], e)
			c.Infof("%v", msg)
			c.Resp().WriteHeader(500)
			fmt.Fprintln(c.Resp(), msg)
		}
	}()
	c.Infof("Running %v", runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name())
	f(c)
	c.Infof("Pass")
}

func test(c gaecontext.HTTPContext) {
	run(c, testMemcacheBasics)
	run(c, testMutex)
}

func init() {
	http.Handle("/", gaecontext.HTTPHandlerFunc(test))
}
