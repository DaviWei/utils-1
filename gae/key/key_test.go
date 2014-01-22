package key

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"appengine_internal"
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
		return New(randomString(), randomString(), rand.Int63(), "")
	}
	return New(randomString(), randomString(), rand.Int63(), randomKey(parents-1))
}

func assertSplit(t *testing.T, source, before, after string) {
	if b, a := split(source, '/'); b != before || a != after {
		t.Fatalf("wrong split %#v => %#v, %#v, wanted %#v, %#v", source, b, a, before, after)
	}
}

func TestSplit(t *testing.T) {
	assertSplit(t, "apapapa/blblbl", "apapapa", "blblbl")
	assertSplit(t, "apa\\/papa/blblbl", "apa\\/papa", "blblbl")
	assertSplit(t, unescape("apa\\/papa"), "apa", "papa")
	assertSplit(t, escape("apa/gapa")+"/"+escape("gnu/hehu"), escape("apa/gapa"), escape("gnu/hehu"))
	assertSplit(t, escape(escape("apa/gapa")+"/"+escape("gnu/hehu"))+"/"+escape(escape("ja/nej")+"/"+escape("yes/no")),
		escape(escape("apa/gapa")+"/"+escape("gnu/hehu")),
		escape(escape("ja/nej")+"/"+escape("yes/no")))
}

func TestEscapeUnescape(t *testing.T) {
	for i := 0; i < 10000; i++ {
		s := randomString()
		e := s
		times := rand.Int() % 20
		for j := 0; j < times; j++ {
			e = escape(e)
		}
		d := e
		for j := 0; j < times; j++ {
			d = unescape(d)
		}
		if d != s {
			t.Fatalf("%#v != %#v", s, s)
		}
	}
}

func TestEncodeString(t *testing.T) {
	buf := &bytes.Buffer{}
	x := "aslfdjasdfasdf"
	writeString(buf, x)
	y, err := readString(buf)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if x != y {
		t.Fatalf("Expected %v, got %v. Buf is %+v", x, y, buf.Bytes())
	}

	buf = &bytes.Buffer{}
	x = ""
	writeString(buf, x)
	y, err = readString(buf)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if x != y {
		t.Fatalf("Expected %v, got %v. Buf is %+v", x, y, buf.Bytes())
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
		t.Fatalf(err.Error())
	}
	if x != y {
		t.Fatalf("Expected %v, got %v. Buf is %+v", x, y, buf.Bytes())
	}

	for i := 1; i < 8; i++ {
		buf = &bytes.Buffer{}
		x = int64(1 << uint((8*i)-1))
		writeInt64(buf, x)
		y, err = readInt64(buf)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if x != y {
			t.Fatalf("Expected %v, got %v. Buf is %+v", x, y, buf.Bytes())
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
			t.Fatalf(err.Error())
		}
		if x != y {
			t.Fatalf("Expected %v, got %v. Buf is %+v", x, y, buf.Bytes())
		}
	}

}

func TestEncodeDecode(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := randomKey(2)
		enc := k.Encode()
		k2, err := Decode(enc)
		if err != nil {
			t.Fatalf("Failed decoding %s: %v", enc, err)
		}
		if !reflect.DeepEqual(k, k2) {
			t.Fatalf("%#v != %#v", k, k2)
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
			t.Fatalf("%+v != %+v", k, k3)
		}
		if !reflect.DeepEqual(k2, k4) {
			t.Fatalf("%+v != %+v", k2, k4)
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
			t.Fatalf(err.Error())
		}
		var i interface{}
		err = json.Unmarshal(enc, &i)
		if err != nil {
			t.Fatalf("Bad json: %#v: %v", string(enc), err.Error())
		}
		w2 := &testWrapper{}
		if err := json.Unmarshal(enc, w2); err != nil {
			t.Fatalf(err.Error())
		}
		if !reflect.DeepEqual(w, w2) {
			t.Fatalf("%+v != %+v", w, w2)
		}
		w3 := &testWrapperString{}
		if err := json.Unmarshal(enc, w3); err != nil {
			t.Fatalf(err.Error())
		}
		k2, err := Decode(w3.Id)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if !k.Equal(k2) {
			t.Fatalf("%v != %v", k, k2)
		}
	}

}

func TestToAndFromJSON(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := randomKey(5)
		enc, err := k.MarshalJSON()
		if err != nil {
			t.Fatalf(err.Error())
		}
		var i interface{}
		err = json.Unmarshal(enc, &i)
		if err != nil {
			t.Fatalf("Bad json: %#v: %v", string(enc), err.Error())
		}
		k2 := Key("")
		if err := k2.UnmarshalJSON(enc); err != nil {
			t.Fatalf(err.Error())
		}
		if !reflect.DeepEqual(k, k2) {
			t.Fatalf("\n%#v\n%#v\n", k, k2)
		}
	}
}

func TestEqual(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := randomKey(6)
		k2 := New(k.Kind(), k.StringID(), k.IntID(), k.Parent())
		if !k.Equal(k2) {
			t.Fatalf("Keys not equal")
		}
	}
}

func TestNilKeys(t *testing.T) {
	var k Key
	var k2 Key
	if !k.Equal(k2) || !k2.Equal(k) {
		t.Fatalf("wth")
	}
	k = randomKey(3)
	if k.Equal(k2) || k2.Equal(k) {
		t.Fatalf("wtf")
	}
}
