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

type KeyElement struct {
	intID    int64
	stringID string
	kind     string
}
type Key []KeyElement

func For(i interface{}, stringId string, intId int64, parent Key) Key {
	val := reflect.ValueOf(i)
	for val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	return New(val.Type().Name(), stringId, intId, parent)
}

func New(kind string, stringId string, intId int64, parent Key) (ret Key) {
	ret = Key{
		KeyElement{
			intID:    intId,
			stringID: stringId,
			kind:     kind,
		},
	}
	if len(parent) > 0 {
		ret = append(ret, parent...)
	}
	return ret
}

func (self Key) String() string {
	buf := &bytes.Buffer{}
	self.describe(buf)
	return string(buf.Bytes())
}

func (self Key) describe(w io.Writer) {
	if len(self) == 0 {
		return
	}
	self.Parent().describe(w)
	child := self[0]
	fmt.Fprintf(w, "/%s,", child.kind)
	if child.stringID != "" {
		fmt.Fprintf(w, "%s", child.stringID)
	}
	if child.intID != 0 {
		fmt.Fprintf(w, "%d", child.intID)
	}
	return
}

func FromGAErr(k *datastore.Key, err error) (result Key, err2 error) {
	err2 = err
	if err2 == nil {
		result = FromGAE(k)
	}
	return
}

func FromGAE(k *datastore.Key) Key {
	if k == nil {
		return nil
	}
	return append(Key{KeyElement{
		kind:     k.Kind(),
		stringID: k.StringID(),
		intID:    k.IntID(),
	}}, FromGAE(k.Parent())...)
}

func (self *Key) GobDecode(b []byte) (err error) {
	*self, err = decode(bytes.NewBuffer(b))
	return
}

func (self Key) GobEncode() ([]byte, error) {
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
	var unmarshalled Key
	if unmarshalled, err = Decode(encoded); err == nil {
		*self = unmarshalled
	}
	return
}

func (self Key) Kind() string {
	return self[0].kind
}

func (self Key) StringID() string {
	return self[0].stringID
}

func (self Key) IntID() int64 {
	return self[0].intID
}

func (self Key) Parent() Key {
	return self[1:]
}

func writeInt64(buf *bytes.Buffer, i int64) (err error) {
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

func writeString(buf *bytes.Buffer, s string) (err error) {
	if err = writeInt64(buf, int64(len(s))); err != nil {
		return
	}
	_, err = fmt.Fprint(buf, s)
	return
}

func readInt64(buf *bytes.Buffer) (i int64, err error) {
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

func readString(buf *bytes.Buffer) (s string, err error) {
	var l int64
	l, err = readInt64(buf)
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

func (self Key) encode(buf *bytes.Buffer) (err error) {
	if len(self) < 1 {
		return
	}
	if err = writeString(buf, self[0].kind); err != nil {
		return
	}
	if err = writeString(buf, self[0].stringID); err != nil {
		return
	}
	if err = writeInt64(buf, self[0].intID); err != nil {
		return
	}
	return self.Parent().encode(buf)
}

func (self Key) Encode() (result string) {
	if self == nil {
		return
	}
	buf := &bytes.Buffer{}
	if err := self.encode(buf); err != nil {
		panic(err)
	}
	return base64.URLEncoding.EncodeToString(buf.Bytes())
}

func (self Key) EncodeEmailId() (result string) {
	return strings.Replace(self.Encode(), "=", "_", -1)
}

func decode(buf *bytes.Buffer) (result Key, err error) {
	el := KeyElement{}
	s := ""
	if s, err = readString(buf); err != nil {
		return
	}
	el.kind = s
	if s, err = readString(buf); err != nil {
		return
	}
	el.stringID = s
	var i int64
	if i, err = readInt64(buf); err != nil {
		return
	}
	el.intID = i
	result = Key{el}
	if buf.Len() > 0 {
		var pres Key
		pres, err = decode(buf)
		if err != nil {
			return
		}
		result = append(result, pres...)
	}
	return
}

func Decode(s string) (result Key, err error) {
	if s == "" {
		return
	}
	b := []byte{}
	if b, err = base64.URLEncoding.DecodeString(s); err != nil {
		return
	}
	buf := bytes.NewBuffer(b)
	return decode(buf)
}

func DecodeEmailId(emailId string) (result Key, err error) {
	return Decode(strings.Replace(emailId, "_", "=", -1))
}

func (self Key) ToGAE(c appengine.Context) *datastore.Key {
	if len(self) < 1 {
		return nil
	}

	return datastore.NewKey(c, self[0].kind, self[0].stringID, self[0].intID, self.Parent().ToGAE(c))
}

func (s KeyElement) Equal(k KeyElement) bool {
	return s.kind == k.kind && s.intID == k.intID && s.stringID == k.stringID
}

func (s Key) Equal(k Key) bool {
	return len(s) == len(k) && (len(s) == 0 || (s[0].Equal(k[0]) && s.Parent().Equal(k.Parent())))
}
