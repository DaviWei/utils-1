package key

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"

	"appengine"
	"appengine/datastore"
)

func split(s string, delim byte) (before, after string) {
	buf := &bytes.Buffer{}
	i := 0
	for i = 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			buf.WriteByte(s[i])
			i++
			buf.WriteByte(s[i])
		case delim:
			before, after = buf.String(), s[i+1:]
			return
		default:
			buf.WriteByte(s[i])
		}
	}
	before = buf.String()
	return
}

func escape(s string) string {
	buf := &bytes.Buffer{}
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case ',':
			buf.WriteString("\\,")
		case '/':
			buf.WriteString("\\/")
		case '\\':
			buf.WriteString("\\\\")
		default:
			buf.WriteByte(s[i])
		}
	}
	return buf.String()
}

func unescape(s string) string {
	buf := &bytes.Buffer{}
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			if i+1 < len(s) {
				i++
				switch s[i] {
				case '\\':
					buf.WriteByte('\\')
				case ',':
					buf.WriteByte(',')
				case '/':
					buf.WriteByte('/')
				default:
					buf.WriteByte('\\')
				}
			} else {
				buf.WriteByte('\\')
			}
		default:
			buf.WriteByte(s[i])
		}
	}
	return buf.String()
}

func For(i interface{}, StringId string, IntId int64, parent Key) Key {
	val := reflect.ValueOf(i)
	for val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	return New(val.Type().Name(), StringId, IntId, parent)
}

type Key string

func New(kind string, stringId string, intId int64, parent Key) (result Key) {
	b := make([]byte, 16)
	used := binary.PutVarint(b, intId)
	return Key(fmt.Sprintf("%v,%v,%v/%v", escape(kind), escape(stringId), escape(string(b[:used])), string(parent)))
}

func (self Key) String() string {
	if len(self) == 0 {
		return ""
	}
	buf := &bytes.Buffer{}
	self.describe(buf)
	return string(buf.Bytes())
}

func (self Key) split() (kind string, stringId string, intId int64, parent Key) {
	rest, after := split(string(self), '/')
	kind, rest = split(rest, ',')
	stringId, rest = split(rest, ',')
	intId, _ = binary.Varint([]byte(unescape(rest)))
	kind, stringId, parent = unescape(kind), unescape(stringId), Key(after)
	return
}

func (self Key) describe(w io.Writer) {
	if len(self) == 0 {
		return
	}
	kind, stringId, intId, parent := self.split()
	parent.describe(w)
	fmt.Fprintf(w, "/%s,", kind)
	if stringId != "" {
		fmt.Fprintf(w, "%s", stringId)
	}
	if intId != 0 {
		fmt.Fprintf(w, "%d", intId)
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
		return Key("")
	}
	return New(k.Kind(), k.StringID(), k.IntID(), FromGAE(k.Parent()))
}

func (self Key) MarshalJSON() (b []byte, err error) {
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

func (self Key) Kind() (result string) {
	result, _, _, _ = self.split()
	return
}

func (self Key) StringID() (result string) {
	_, result, _, _ = self.split()
	return
}

func (self Key) IntID() (result int64) {
	_, _, result, _ = self.split()
	return
}

func (self Key) Parent() (result Key) {
	_, _, _, result = self.split()
	return
}

func (self Key) Encode() (result string) {
	return strings.Replace(base64.URLEncoding.EncodeToString([]byte(self)), "=", ".", -1)
}

func Decode(s string) (result Key, err error) {
	if s == "" {
		return
	}
	b := []byte{}
	b, err = base64.URLEncoding.DecodeString(strings.Replace(s, ".", "=", -1))
	if err != nil {
		return
	}
	result = Key(string(b))
	return
}

func (self Key) ToGAE(c appengine.Context) *datastore.Key {
	if len(self) < 1 {
		return nil
	}
	kind, stringId, intId, parent := self.split()
	return datastore.NewKey(c, kind, stringId, intId, Key(parent).ToGAE(c))
}

func (s Key) Equal(k Key) bool {
	return s == k
}
