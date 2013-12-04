package key

import (
	"appengine"
	"appengine/datastore"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
)

type Key struct {
	parent   *Key
	intID    int64
	stringID string
	kind     string
}

func For(i interface{}, stringId string, intId int64, parent *Key) *Key {
	val := reflect.ValueOf(i)
	for val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	return New(val.Type().Name(), stringId, intId, parent)
}

func New(kind string, stringId string, intId int64, parent *Key) *Key {
	return &Key{
		parent:   parent,
		intID:    intId,
		stringID: stringId,
		kind:     kind,
	}
}

func (self *Key) String() string {
	buf := &bytes.Buffer{}
	self.describe(buf)
	return string(buf.Bytes())
}

func (self *Key) describe(w io.Writer) {
	if self == nil {
		return
	}
	self.parent.describe(w)
	fmt.Fprintf(w, "/%s,", self.kind)
	if self.stringID != "" {
		fmt.Fprintf(w, "%s", self.stringID)
	}
	if self.intID != 0 {
		fmt.Fprintf(w, "%d", self.intID)
	}
	return
}

func FromGAErr(k *datastore.Key, err error) (result *Key, err2 error) {
	err2 = err
	if err2 == nil {
		result = FromGAE(k)
	}
	return
}

func FromGAE(k *datastore.Key) *Key {
	if k == nil {
		return nil
	}
	return &Key{
		kind:     k.Kind(),
		stringID: k.StringID(),
		intID:    k.IntID(),
		parent:   FromGAE(k.Parent()),
	}
}

func (self *Key) GobDecode(b []byte) error {
	return self.decode(bytes.NewBuffer(b))
}

func (self *Key) GobEncode() ([]byte, error) {
	buf := &bytes.Buffer{}
	err := self.encode(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

func (self *Key) MarshalJSON() (b []byte, err error) {
	return json.Marshal(self.Encode())
}

func (self *Key) UnmarshalJSON(b []byte) (err error) {
	encoded := ""
	if err = json.Unmarshal(b, &encoded); err != nil {
		return
	}
	var unmarshalled *Key
	if unmarshalled, err = Decode(encoded); err == nil {
		*self = *unmarshalled
	}
	return
}

func (self *Key) Kind() string {
	return self.kind
}

func (self *Key) StringID() string {
	return self.stringID
}

func (self *Key) IntID() int64 {
	return self.intID
}

func (self *Key) Parent() *Key {
	return self.parent
}

func (self *Key) writeInt64(buf *bytes.Buffer, i int64) (err error) {
	count := byte(0)
	zeroes := byte(0)
	nonZeroesSeen := false
	tmpBuf := &bytes.Buffer{}
	ui := uint64(i)
	b := byte(0)
	for ui != 0 {
		b = byte(ui)
		if !nonZeroesSeen && b == 0 {
			zeroes++
		} else {
			nonZeroesSeen = true
			if err = tmpBuf.WriteByte(byte(ui)); err != nil {
				return
			}
		}
		ui = ui >> 8
		count++
	}
	count += 10 * zeroes
	if err = buf.WriteByte(count); err != nil {
		return
	}
	_, err = io.Copy(buf, tmpBuf)
	return
}

func (self *Key) writeString(buf *bytes.Buffer, s string) (err error) {
	if err = self.writeInt64(buf, int64(len(s))); err != nil {
		return
	}
	_, err = fmt.Fprint(buf, s)
	return
}

func (self *Key) readInt64(buf *bytes.Buffer) (i int64, err error) {
	var l byte
	if l, err = buf.ReadByte(); err != nil {
		return
	}
	zeroes := byte(0)
	for l > 9 {
		l -= 10
		zeroes++
	}
	var b byte
	var ui uint64
	for n := byte(0) + zeroes; n < l; n++ {
		if b, err = buf.ReadByte(); err != nil {
			return
		}
		ui += (uint64(b) << (8 * n))
	}
	i = int64(ui)
	return
}

func (self *Key) readString(buf *bytes.Buffer) (s string, err error) {
	var l int64
	l, err = self.readInt64(buf)
	if err != nil {
		return
	}
	b := make([]byte, l)
	var r int
	r, err = buf.Read(b)
	if int64(r) != l {
		err = fmt.Errorf("Wanted to read %v, but got %v", l, r)
	}
	if err != nil {
		return
	}
	s = string(b)
	return
}

func (self *Key) encode(buf *bytes.Buffer) (err error) {
	if self == nil {
		return
	}
	if err = self.writeString(buf, self.kind); err != nil {
		return
	}
	if err = self.writeString(buf, self.stringID); err != nil {
		return
	}
	if err = self.writeInt64(buf, self.intID); err != nil {
		return
	}
	err = self.parent.encode(buf)
	return
}

func (self *Key) Encode() (result string) {
	if self == nil {
		return
	}
	buf := &bytes.Buffer{}
	if err := self.encode(buf); err != nil {
		panic(err)
	}
	result = base64.URLEncoding.EncodeToString(buf.Bytes())
	return
}

func (self *Key) EncodeEmailId() (result string) {
	return strings.Replace(self.Encode(), "=", "_", -1)
}

func (self *Key) decode(buf *bytes.Buffer) (err error) {
	s := ""
	if s, err = self.readString(buf); err != nil {
		return
	}
	self.kind = s
	if s, err = self.readString(buf); err != nil {
		return
	}
	self.stringID = s
	var i int64
	if i, err = self.readInt64(buf); err != nil {
		return
	}
	self.intID = i
	if buf.Len() > 0 {
		self.parent = &Key{}
		err = self.parent.decode(buf)
	}
	return
}

func Decode(s string) (result *Key, err error) {
	if s == "" {
		return
	}
	b := []byte{}
	if b, err = base64.URLEncoding.DecodeString(s); err != nil {
		return
	}
	buf := bytes.NewBuffer(b)
	result = &Key{}
	err = result.decode(buf)
	return
}

func DecodeEmailId(emailId string) (result *Key, err error) {
	return Decode(strings.Replace(emailId, "_", "=", -1))
}

func (self *Key) ToGAE(c appengine.Context) *datastore.Key {
	if self == nil {
		return nil
	}

	return datastore.NewKey(c, self.kind, self.stringID, self.intID, self.parent.ToGAE(c))
}

func (self *Key) Equal(k *Key) bool {
	if self == nil {
		return k == nil
	}
	return k != nil && self.kind == k.kind && self.intID == k.intID && self.stringID == k.stringID && self.parent.Equal(k.parent)
}
