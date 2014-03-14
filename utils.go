package utils

import (
	"bytes"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"text/template"
	"time"

	"github.com/soundtrackyourbrand/utils/run"
	"github.com/soundtrackyourbrand/utils/key"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const (
	randomChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func RandomString(i int) string {
	buf := new(bytes.Buffer)
	for buf.Len() < i {
		fmt.Fprintf(buf, "%c", randomChars[rand.Intn(len(randomChars))])
	}
	return string(buf.Bytes())
}

func Prettify(obj interface{}) string {
	b, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return fmt.Sprintf("%+v", obj)
	}
	return string(b)
}

func InSlice(slice interface{}, needle interface{}) (result bool, err error) {
	sliceValue := reflect.ValueOf(slice)
	if sliceValue.Kind() != reflect.Slice {
		err = fmt.Errorf("%#v is not a slice", slice)
	}
	if sliceValue.Type().Elem() != reflect.TypeOf(needle) {
		err = fmt.Errorf("%#v is a slice of %#v", slice, needle)
	}
	for i := 0; i < sliceValue.Len(); i++ {
		if reflect.DeepEqual(sliceValue.Index(i).Interface(), needle) {
			result = true
			return
		}
	}
	return
}

func ReflectCopy(source, destinationPointer interface{}) {
	srcValue := reflect.ValueOf(source)
	if reflect.PtrTo(reflect.TypeOf(source)) == reflect.TypeOf(destinationPointer) {
		reflect.ValueOf(destinationPointer).Elem().Set(srcValue)
	} else {
		reflect.ValueOf(destinationPointer).Elem().Set(reflect.Indirect(srcValue))
	}
}

type AccessToken interface {
	Encode() ([]byte, error)
	Scopes() []string
}

type tokenEnvelope struct {
	ExpiresAt time.Time
	Hash      []byte
	Token     AccessToken
}

var secret []byte
var accessTokenType reflect.Type

func ParseAccessTokens(s []byte, token AccessToken) {
	secret = s
	accessTokenType = reflect.TypeOf(token)
	if accessTokenType.Kind() != reflect.Ptr || accessTokenType.Elem().Kind() != reflect.Struct {
		panic(fmt.Errorf("%v is not a pointer to a struct", token))
	}
	gob.Register(token)
}

func EncodeToken(token AccessToken, timeout time.Duration) (result string, err error) {
	envelope := &tokenEnvelope{
		ExpiresAt: time.Now().Add(timeout),
		Token:     token,
	}
	h, err := envelope.generateHash()
	if err != nil {
		return
	}
	envelope.Hash = h
	b := &bytes.Buffer{}
	b64Enc := base64.NewEncoder(base64.URLEncoding, b)
	gobEnc := gob.NewEncoder(b64Enc)
	if err = gobEnc.Encode(envelope); err != nil {
		return
	}
	if err = b64Enc.Close(); err != nil {
		return
	}
	result = strings.Replace(string(b.Bytes()), "=", ".", -1)
	return
}

func (self *tokenEnvelope) generateHash() (result []byte, err error) {
	hash := sha512.New()
	tokenCode, err := self.Token.Encode()
	if err != nil {
		return
	}
	if _, err = hash.Write(tokenCode); err != nil {
		return
	}
	if _, err = hash.Write(secret); err != nil {
		return
	}
	result = hash.Sum(nil)
	return
}

/*
ParseAccessToken will return the AccessToken encoded in d. If dst is provided it will encode into it.
*/
func ParseAccessToken(d string, dst AccessToken) (result AccessToken, err error) {
	if dst == nil {
		dst = reflect.New(accessTokenType.Elem()).Interface().(AccessToken)
	}
	result = dst
	envelope := &tokenEnvelope{}
	dec := gob.NewDecoder(base64.NewDecoder(base64.URLEncoding, bytes.NewBufferString(strings.Replace(d, ".", "=", -1))))
	if err = dec.Decode(&envelope); err != nil {
		err = fmt.Errorf("Invalid AccessToken: %v, %v", d, err)
		return
	}
	if envelope.ExpiresAt.Before(time.Now()) {
		err = fmt.Errorf("Expired AccessToken: %v", envelope)
		return
	}
	wantedHash, err := envelope.generateHash()
	if err != nil {
		return
	}
	if len(wantedHash) != len(envelope.Hash) || subtle.ConstantTimeCompare(envelope.Hash, wantedHash) != 1 {
		err = fmt.Errorf("Invalid AccessToken: hash of %+v should be %v but was %v", envelope.Token, hex.EncodeToString(envelope.Hash), hex.EncodeToString(wantedHash))
		return
	}
	dstVal := reflect.ValueOf(dst)
	tokenVal := reflect.ValueOf(envelope.Token)
	if dstVal.Kind() != reflect.Ptr {
		err = fmt.Errorf("%#v is not a pointer", dst)
		return
	}
	if tokenVal.Kind() != reflect.Ptr {
		err = fmt.Errorf("%#v is not a pointer", tokenVal.Interface())
		return
	}
	if dstVal.Type() != tokenVal.Type() {
		err = fmt.Errorf("Can't load a %v into a %v", tokenVal.Type(), dstVal.Type())
		return
	}
	dstVal.Elem().Set(tokenVal.Elem())
	return
}

func ValidateFuncOutput(f interface{}, out []reflect.Type) error {
	fVal := reflect.ValueOf(f)
	if fVal.Kind() != reflect.Func {
		return fmt.Errorf("%v is not a func", f)
	}
	fType := fVal.Type()
	if fType.NumOut() != len(out) {
		return fmt.Errorf("%v should take %v arguments", f, len(out))
	}
	for index, outType := range out {
		if !fType.Out(index).AssignableTo(outType) {
			return fmt.Errorf("Return value %v for %v (%v) should be assignable to %v", index, f, fType.Out(index), outType)
		}
	}
	return nil
}

func ValidateFuncOutputs(f interface{}, outs ...[]reflect.Type) (errs []error) {
	for _, out := range outs {
		if err := ValidateFuncOutput(f, out); err != nil {
			errs = append(errs, err)
		}
	}
	return
}

func ValidateFuncInput(f interface{}, in []reflect.Type) error {
	fVal := reflect.ValueOf(f)
	if fVal.Kind() != reflect.Func {
		return fmt.Errorf("%v is not a func", f)
	}
	fType := fVal.Type()
	if fType.NumIn() != len(in) {
		return fmt.Errorf("%v should take %v arguments", f, len(in))
	}
	for index, inType := range in {
		if !fType.In(index).AssignableTo(inType) {
			return fmt.Errorf("Argument %v for %v (%v) should be assignable to %v", index, f, fType.In(index), inType)
		}
	}
	return nil
}

func ValidateFuncInputs(f interface{}, ins ...[]reflect.Type) (errs []error) {
	for _, in := range ins {
		if err := ValidateFuncInput(f, in); err != nil {
			errs = append(errs, err)
		}
	}
	return
}

func Example(t reflect.Type) (result interface{}) {
	return example(t, map[string]int{})
}

func example(t reflect.Type, seen map[string]int) (result interface{}) {
	seen[t.Name()]++
	switch t.Kind() {
	case reflect.Slice:
		val := reflect.MakeSlice(t, 1, 1)
		result = val.Interface()
		if seen[t.Name()] > 2 {
			return
		}
		val.Index(0).Set(reflect.ValueOf(example(t.Elem(), seen)))
		result = val.Interface()
	case reflect.Ptr:
		val := reflect.New(t.Elem())
		result = val.Interface()
		if seen[t.Name()] > 2 {
			return
		}
		x := example(t.Elem(), seen)
		val.Elem().Set(reflect.ValueOf(x))
		result = val.Interface()
	case reflect.Interface:
		result = struct{}{}
	case reflect.String:
		result = reflect.ValueOf("[...]").Convert(t).Interface()
	case reflect.Int:
		result = reflect.ValueOf(1).Convert(t).Interface()
	case reflect.Int64:
		result = reflect.ValueOf(int64(1)).Convert(t).Interface()
	case reflect.Float64:
		result = reflect.ValueOf(float64(1)).Convert(t).Interface()
	case reflect.Bool:
		result = reflect.ValueOf(true).Convert(t).Interface()
	default:
		val := reflect.New(t)
		result = val.Elem().Interface()
		if seen[t.Name()] > 2 {
			return
		}
		if t.Kind() == reflect.Struct {
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				if field.PkgPath == "" {
					val.Elem().Field(i).Set(reflect.ValueOf(example(field.Type, seen)))
				}
			}
		}
		result = val.Elem().Interface()
	}
	return
}

var revisionTemplate = template.Must(template.New("").Parse(`package common

const (
	GitRevision = "{{.Revision}}"
	GitBranch = "{{.Branch}}"
)`))

func GitRevision(dir string) (rev string, err error) {
	revisionResult, _, err := run.RunAndReturn("git", "--git-dir", filepath.Join(dir, ".git"), "--work-tree", dir, "rev-parse", "HEAD")
	if err != nil {
		return
	}
	rev = strings.TrimSpace(revisionResult)
	return
}

func GitBranch(dir string) (branch string, err error) {
	branchResult, _, err := run.RunAndReturn("git", "--git-dir", filepath.Join(dir, ".git"), "--work-tree", dir, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return
	}
	branch = strings.TrimSpace(branchResult)
	return
}

func UpdateGitRevision(dir, destination string) (err error) {
	rev, err := GitRevision(dir)
	if err != nil {
		return
	}
	branch, err := GitBranch(dir)
	if err != nil {
		return
	}
	tmpDest := fmt.Sprintf("%v_%v", destination, rand.Int63())
	outfile, err := os.Create(tmpDest)
	if err != nil {
		return
	}
	if err = revisionTemplate.Execute(outfile, map[string]interface{}{
		"Revision": rev,
		"Branch":   branch,
	}); err != nil {
		return
	}
	if err = outfile.Close(); err != nil {
		return
	}
	if err = os.Rename(tmpDest, destination); err != nil {
		return
	}
	return
}

type JSONTime time.Time

const (
	ISO8601DayTimeFormat  = "150405"
	ISO8601DateTimeFormat = "20060102150405"
	ISO8601DateFormat     = "20060102"
)

func (self JSONTime) MarshalJSON() ([]byte, error) {
	if time.Time(self).IsZero() {
		return json.Marshal(nil)
	}
	return json.Marshal(time.Time(self).Format(ISO8601DateTimeFormat))
}

func (self *JSONTime) UnmarshalJSON(b []byte) (err error) {
	var s string
	if err = json.Unmarshal(b, &s); err == nil {
		if s != "" {
			var t time.Time
			if t, err = time.Parse(ISO8601DateTimeFormat, s); err == nil {
				*self = JSONTime(t)
			}
		} else {
			*self = JSONTime(time.Time{})
		}
	}
	return
}

type Attachment struct {
	ContentID string
	Name      string
	Data      []byte
}

type MailType string

type EmailTemplateSender interface {
	SendEmailTemplate(recipient string, mailContext map[string]interface{}, templateName MailType, locale string, attachments []Attachment, accountId *key.Key) (err error)
	SendEmailTemplateFromSender(recipient string, mailContext map[string]interface{}, templateName MailType, locale string, attachments []Attachment, senderAddress string, accountId *key.Key) (err error)
}

type ByteString struct {
	Bytes []byte
}

func (self ByteString) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(self.Bytes))
}

func (self ByteString) String() string {
	return string(self.Bytes)
}

func (self *ByteString) UnmarshalJSON(b []byte) error {
	s := ""
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	self.Bytes = []byte(s)
	return nil
}
