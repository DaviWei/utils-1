package key

import (
	"appengine_internal"
	"bytes"
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type dummyContext struct {
	AppID string
}

func (self dummyContext) Debugf(s string, args ...interface{})    {}
func (self dummyContext) Infof(s string, args ...interface{})     {}
func (self dummyContext) Warningf(s string, args ...interface{})  {}
func (self dummyContext) Criticalf(s string, args ...interface{}) {}
func (self dummyContext) Errorf(s string, args ...interface{})    {}
func (self dummyContext) Call(service, method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error {
	return nil
}
func (self dummyContext) FullyQualifiedAppID() string { return self.AppID }
func (self dummyContext) Request() interface{}        { return nil }

func randomString() string {
	buf := make([]byte, 15)
	for index := range buf {
		buf[index] = byte(rand.Int())
	}
	return string(buf)
}

func randomKey(parents int) Key {
	if parents == 0 {
		return New(randomString(), randomString(), rand.Int63(), nil)
	}
	return New(randomString(), randomString(), rand.Int63(), randomKey(parents-1))
}

func TestEncodeString(t *testing.T) {
	buf := &bytes.Buffer{}
	x := "aslfdjasdfasdf"
	writeString(buf, x)
	y, err := readString(buf)
	if err != nil {
		t.Errorf(err.Error())
	}
	if x != y {
		t.Errorf("Expected %v, got %v. Buf is %+v", x, y, buf.Bytes())
	}

	buf = &bytes.Buffer{}
	x = ""
	writeString(buf, x)
	y, err = readString(buf)
	if err != nil {
		t.Errorf(err.Error())
	}
	if x != y {
		t.Errorf("Expected %v, got %v. Buf is %+v", x, y, buf.Bytes())
	}
}

func TestEncodeInt64(t *testing.T) {
	var buf *bytes.Buffer
	var x int64
	var y int64
	var err error

	buf = &bytes.Buffer{}
	x = int64(0)
	writeInt64(buf, x)
	y, err = readInt64(buf)
	if err != nil {
		t.Errorf(err.Error())
	}
	if x != y {
		t.Errorf("Expected %v, got %v. Buf is %+v", x, y, buf.Bytes())
	}

	for i := 1; i < 8; i++ {
		buf = &bytes.Buffer{}
		x = int64(1 << uint((8*i)-1))
		writeInt64(buf, x)
		y, err = readInt64(buf)
		if err != nil {
			t.Errorf(err.Error())
		}
		if x != y {
			t.Errorf("Expected %v, got %v. Buf is %+v", x, y, buf.Bytes())
		}
	}

	for i := 0; i < 10; i++ {
		buf = &bytes.Buffer{}
		x = rand.Int63()
		if (rand.Int() % 2) == 0 {
			x = -x
		}
		writeInt64(buf, x)
		y, err = readInt64(buf)
		if err != nil {
			t.Errorf(err.Error())
		}
		if x != y {
			t.Errorf("Expected %v, got %v. Buf is %+v", x, y, buf.Bytes())
		}
	}

}

func TestEncodeDecode(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := randomKey(5)
		enc := k.Encode()
		k2, err := Decode(enc)
		if err != nil {
			t.Errorf("Failed decoding %s: %v", enc, err)
		}
		if !reflect.DeepEqual(k, k2) {
			t.Errorf("%+v != %+v", k, k2)
		}
	}
}

func TestFromAndToGAE(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := randomKey(3)
		k2 := k.ToGAE(dummyContext{"myapp"})
		k3 := FromGAE(k2)
		k4 := k3.ToGAE(dummyContext{"myapp"})
		if !reflect.DeepEqual(k, k3) {
			t.Errorf("%+v != %+v", k, k3)
		}
		if !reflect.DeepEqual(k2, k4) {
			t.Errorf("%+v != %+v", k2, k4)
		}
	}
}

type testWrapper struct {
	Id   Key
	Name string
}

type testWrapperString struct {
	Id   string
	Name string
}

func TestToAndFromJSONInsideWrapper(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := randomKey(5)
		w := &testWrapper{
			Id:   k,
			Name: "hehu",
		}
		enc, err := json.Marshal(w)
		if err != nil {
			t.Errorf(err.Error())
		}
		var i interface{}
		err = json.Unmarshal(enc, &i)
		if err != nil {
			t.Errorf("Bad json: %#v: %v", string(enc), err.Error())
		}
		w2 := &testWrapper{}
		if err := json.Unmarshal(enc, w2); err != nil {
			t.Errorf(err.Error())
		}
		if !reflect.DeepEqual(w, w2) {
			t.Errorf("%+v != %+v", w, w2)
		}
		w3 := &testWrapperString{}
		if err := json.Unmarshal(enc, w3); err != nil {
			t.Errorf(err.Error())
		}
		k2, err := Decode(w3.Id)
		if err != nil {
			t.Errorf(err.Error())
		}
		if !k.Equal(k2) {
			t.Errorf("%v != %v", k, k2)
		}
	}

}

func TestToAndFromJSON(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := randomKey(5)
		enc, err := k.MarshalJSON()
		if err != nil {
			t.Errorf(err.Error())
		}
		var i interface{}
		err = json.Unmarshal(enc, &i)
		if err != nil {
			t.Errorf("Bad json: %#v: %v", string(enc), err.Error())
		}
		k2 := Key{}
		if err := k2.UnmarshalJSON(enc); err != nil {
			t.Errorf(err.Error())
		}
		if !reflect.DeepEqual(k, k2) {
			t.Errorf("\n%#v\n%#v\n", k, k2)
		}
	}
}

func TestEqual(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := randomKey(6)
		k2 := New(k.Kind(), k.StringID(), k.IntID(), k.Parent())
		if !k.Equal(k2) {
			t.Errorf("Keys not equal")
		}
	}
}

func TestNilKeys(t *testing.T) {
	var k Key
	var k2 Key
	if !k.Equal(k2) || !k2.Equal(k) {
		t.Errorf("wth")
	}
	k = randomKey(3)
	if k.Equal(k2) || k2.Equal(k) {
		t.Errorf("wtf")
	}
}
