package web

import (
	"fmt"
	"net/http"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/soundtrackyourbrand/utils"
	"github.com/soundtrackyourbrand/utils/gae"
	"github.com/soundtrackyourbrand/utils/gae/gaecontext"
	"github.com/soundtrackyourbrand/utils/gae/key"
	"github.com/soundtrackyourbrand/utils/gae/memcache"
	"github.com/soundtrackyourbrand/utils/gae/mutex"
	"github.com/soundtrackyourbrand/utils/web/jsoncontext"
)

type Token struct {
	Name string
}

func (self Token) Encode() ([]byte, error) {
	return []byte(self.Name), nil
}

func (self Token) Scopes() []string {
	return []string{"basic"}
}

func init() {
	utils.ParseAccessTokens([]byte("so secret"), &Token{})
}

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
	Id        key.Key `datastore:"-"`
	Name      string
	Foreign   key.Key
	Age       int
	Processes []string
}

func (self *ts) GetId() key.Key {
	return self.Id
}

func (self *ts) SetId(id key.Key) {
	self.Id = id
}

func (self *ts) Equal(o *ts) bool {
	return self.Id.Equal(o.Id) && self.Name == o.Name && self.Age == o.Age
}

func (self *ts) AfterLoad(c gaecontext.HTTPContext) (err error) {
	self.Processes = append(self.Processes, "AfterLoad")
	return
}

func (self *ts) AfterDelete(c gaecontext.HTTPContext) (err error) {
	self.Processes = append(self.Processes, "AfterDelete")
	return
}

func (self *ts) AfterSave(c gaecontext.HTTPContext) (err error) {
	self.Processes = append(self.Processes, "AfterSave")
	return
}

func (self *ts) AfterCreate(c gaecontext.HTTPContext) (err error) {
	self.Processes = append(self.Processes, "AfterCreate")
	return
}

func (self *ts) AfterUpdate(c gaecontext.HTTPContext) (err error) {
	self.Processes = append(self.Processes, "AfterUpdate")
	return
}

func (self *ts) BeforeSave(c gaecontext.HTTPContext) (err error) {
	self.Processes = append(self.Processes, "BeforeSave")
	return
}

func (self *ts) BeforeCreate(c gaecontext.HTTPContext) (err error) {
	self.Processes = append(self.Processes, "BeforeCreate")
	return
}

func (self *ts) BeforeUpdate(c gaecontext.HTTPContext) (err error) {
	self.Processes = append(self.Processes, "BeforeUpdate")
	return
}

var findTsByName = gae.Finder(&ts{}, "Name")
var findTsByForeign = gae.Finder(&ts{}, "Foreign")
var findTsByAncestorAndName = gae.AncestorFinder(&ts{}, "Name")
var findTsByAncestorAndForeign = gae.AncestorFinder(&ts{}, "Foreign")

func testGet(c gaecontext.HTTPContext) {
	gae.DelAll(c, &ts{})
	k, err := key.For(&ts{}, "", 0, "")
	if err != nil {
		panic(err)
	}
	t := &ts{
		Id:   k,
		Name: "the t",
		Age:  12,
	}
	if err := gae.Put(c, t); err != nil {
		panic(err)
	}
	wantedProcesses := []string{"BeforeCreate", "BeforeSave", "AfterCreate", "AfterSave"}
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
	wantedProcesses = []string{"BeforeCreate", "BeforeSave", "AfterLoad"}
	if !reflect.DeepEqual(t2.Processes, wantedProcesses) {
		panic(fmt.Sprintf("wrong processes! wanted %+v but got %+v", wantedProcesses, t2.Processes))
	}
	t2.Age = 13
	if err := gae.Put(c, t2); err != nil {
		panic(err)
	}
	wantedProcesses = append(wantedProcesses, "BeforeUpdate", "BeforeSave", "AfterUpdate", "AfterSave")
	if !reflect.DeepEqual(t2.Processes, wantedProcesses) {
		panic("wrong processes!")
	}
	if err := gae.Del(c, t2); err != nil {
		panic(err)
	}
	wantedProcesses = append(wantedProcesses, "AfterDelete")
	if !reflect.DeepEqual(t2.Processes, wantedProcesses) {
		panic("wrong processes")
	}
}

func testAncestorFindByKey(c gaecontext.HTTPContext) {
	gae.DelAll(c, &ts{})
	parentKey, err := key.New("Parent", "gnu", 0, "")
	if err != nil {
		panic(err)
	}
	foreign, err := key.New("Secondary", "sec", 0, "")
	if err != nil {
		panic(err)
	}
	k, err := key.For(&ts{}, "", 0, parentKey)
	if err != nil {
		panic(err)
	}
	t2 := &ts{
		Id:      k,
		Name:    "t again",
		Age:     14,
		Foreign: foreign,
	}
	if err := gae.Put(c, t2); err != nil {
		panic(err)
	}
	res := []ts{}
	if err := findTsByAncestorAndForeign(c, &res, parentKey, foreign); err != nil {
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

func testAncestorFind(c gaecontext.HTTPContext) {
	gae.DelAll(c, &ts{})
	parentKey, err := key.New("Parent", "gnu", 0, "")
	if err != nil {
		panic(err)
	}
	k, err := key.For(&ts{}, "", 0, parentKey)
	if err != nil {
		panic(err)
	}
	t2 := &ts{
		Id:   k,
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

func testFindByKey(c gaecontext.HTTPContext) {
	gae.DelAll(c, &ts{})
	foreignParent, err := key.New("ForeignParent", "anothername", 0, "")
	if err != nil {
		panic(err)
	}
	foreign, err := key.New("Foreign", "name", 0, foreignParent)
	if err != nil {
		panic(err)
	}
	notForeign, err := key.New("Foreign", "name2", 0, "")
	if err != nil {
		panic(err)
	}
	k, err := key.For(&ts{}, "", 0, "")
	if err != nil {
		panic(err)
	}
	t2 := &ts{
		Id:      k,
		Name:    "another t",
		Age:     14,
		Foreign: foreign,
	}
	if err := gae.Put(c, t2); err != nil {
		panic(err)
	}
	time.Sleep(time.Second)
	res := []ts{}
	if err := findTsByForeign(c, &res, notForeign); err != nil {
		panic(err)
	}
	if len(res) != 0 {
		panic("should be empty")
	}
	if err := findTsByForeign(c, &res, foreign); err != nil {
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
	gae.DelAll(c, &ts{})
	id, err := key.For(&ts{}, "", 0, "")
	if err != nil {
		panic(err)
	}
	t2 := &ts{
		Id:   id,
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

func testMemcacheDeletion(c gaecontext.HTTPContext) {
	t1 := &ts{}
	parentKey, err := key.For(t1, "parent", 0, "")
	if err != nil {
		panic(err)
	}
	t1.Id, err = key.For(t1, "", 0, parentKey)
	if err != nil {
		panic(err)
	}
	t1.Name = "hej"
	if err := gae.Put(c, t1); err != nil {
		panic(err)
	}
	t2 := &ts{Id: t1.Id}
	if err := gae.GetById(c, t2); err != nil {
		panic(err)
	}
	if t2.Name != "hej" {
		panic("wrong name")
	}

	time.Sleep(time.Second)
	found := []ts{}
	if err := findTsByName(c, &found, "hej"); err != nil {
		panic(err)
	}
	if len(found) != 1 {
		panic("wrong len")
	}
	if found[0].Name != "hej" {
		panic("wrong name")
	}
	found = []ts{}
	if err := findTsByAncestorAndName(c, &found, parentKey, "hej"); err != nil {
		panic(err)
	}
	if len(found) != 1 {
		panic("wrong len")
	}
	if found[0].Name != "hej" {
		panic("wrong name")
	}

	t1.Name = "hehu"
	if err := gae.Put(c, t1); err != nil {
		panic(err)
	}

	time.Sleep(time.Second)
	found = []ts{}
	if err := findTsByName(c, &found, "hej"); err != nil {
		panic(err)
	}
	if len(found) != 0 {
		panic("wrong len")
	}
	found = []ts{}
	if err := findTsByAncestorAndName(c, &found, parentKey, "hej"); err != nil {
		panic(err)
	}
	if len(found) != 0 {
		panic("wrong len")
	}
	found = []ts{}
	if err := findTsByName(c, &found, "hehu"); err != nil {
		panic(err)
	}
	if len(found) != 1 {
		panic("wrong len")
	}
	if found[0].Name != "hehu" {
		panic("wrong name")
	}
	found = []ts{}
	if err := findTsByAncestorAndName(c, &found, parentKey, "hehu"); err != nil {
		panic(err)
	}
	if len(found) != 1 {
		panic("wrong len")
	}
	if found[0].Name != "hehu" {
		panic("wrong name")
	}

	if err := c.Transaction(func(c gaecontext.HTTPContext) (err error) {
		t1.Name = "blapp"
		err = gae.Put(c, t1)
		return
	}, false); err != nil {
		panic(err)
	}

	time.Sleep(time.Second)
	found = []ts{}
	if err := findTsByName(c, &found, "hehu"); err != nil {
		panic(err)
	}
	if len(found) != 0 {
		panic("wrong len")
	}
	found = []ts{}
	if err := findTsByAncestorAndName(c, &found, parentKey, "hehu"); err != nil {
		panic(err)
	}
	if len(found) != 0 {
		panic("wrong len")
	}
	found = []ts{}
	if err := findTsByName(c, &found, "blapp"); err != nil {
		panic(err)
	}
	if len(found) != 1 {
		panic("wrong len")
	}
	if found[0].Name != "blapp" {
		panic("wrong name")
	}
	found = []ts{}
	if err := findTsByAncestorAndName(c, &found, parentKey, "blapp"); err != nil {
		panic(err)
	}
	if len(found) != 1 {
		panic("wrong len")
	}
	if found[0].Name != "blapp" {
		panic("wrong name")
	}

}

func testAccessTokens(c gaecontext.HTTPContext) {
	enc, err := utils.EncodeToken(&Token{Name: "hehu"}, time.Hour)
	if err != nil {
		panic(err)
	}
	c.Req().Header.Set("Authorization", fmt.Sprintf("Bearer %v", enc))
	tok := &Token{}
	if _, err := c.AccessToken(tok); err != nil {
		panic(err)
	}
	if tok.Name != "hehu" {
		panic("wrong name!")
	}
	enc, err = utils.EncodeToken(&Token{Name: "hehu"}, time.Millisecond)
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Millisecond * 5)
	c.Req().Header.Set("Authorization", fmt.Sprintf("Bearer %v", enc))
	if _, err := c.AccessToken(tok); !strings.Contains(err.Error(), "Expired") {
		panic("should be expired")
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

	run(c, testAncestorFindByKey)
	run(c, testFindByKey)
	run(c, testMemcacheBasics)
	run(c, testMutex)
	run(c, testGet)
	run(c, testFind)
	run(c, testAncestorFind)
	run(c, testAccessTokens)
	run(c, testMemcacheDeletion)
	return nil
}

type Query struct {
	Offset int
	Limit  int
}

type User struct {
	Name  string
	Email string
}

type Users []User

func getUsers(c gaecontext.JSONContext, q Query) (status int, result Users, err error) {
	result = Users{
		User{
			Name:  "charlie brown",
			Email: "charlie@brown.net",
		},
	}
	return
}

func getUser(c gaecontext.JSONContext, q Query) (status int, result *User, err error) {
	result = &User{
		Name:  "hehu",
		Email: "blabl@bla.bla",
	}
	return
}

func init() {
	router := mux.NewRouter()
	router.Path("/").Handler(gaecontext.HTTPHandlerFunc(test))
	gaecontext.DocHandle(router, getUsers, "/api/users", "GET", 0)
	gaecontext.DocHandle(router, getUser, "/api/user", "GET", 0, "basic")
	router.Handle("/doc", jsoncontext.DefaultDocHandler)
	http.Handle("/", router)
}
