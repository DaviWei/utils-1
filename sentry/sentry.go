package sentry

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/soundtrackyourbrand/utils"
)

type Sentry struct {
	projectId  string
	url        string
	authHeader string
	client     http.Client
}

type Tag struct {
	Key   string
	Value string
}

type Tags []Tag

type Severity string

const (
	// Accepted severity levels by Sentry
	DEBUG   Severity = "debug"
	INFO             = "info"
	WARNING          = "warning"
	ERROR            = "error"
	FATAL            = "fatal"
)

type Packet struct {
	// Required
	EventId string `json:"event_id"` // Unique id, max 32 characters
	//	Project   string    `json:"project"`
	Timestamp time.Time `json:"timestamp"` // Sentry assumes it is given in UTC. Use the ISO 8601 format
	Message   string    `json:"message"`   // Human-readable message, max length 100 characters
	Level     Severity  `json:"level"`     // Defaults to "error"
	Logger    string    `json:"logger"`

	// Optional
	Culprit    string `json:"culprit, omitempty"` // E.g. function name
	Tags       Tags   `json:"tags,omitempty"`
	ServerName string `json:"server_name,omitempty"`
}

func New(client *http.Client, dsn string, tags map[string]string) (result *Sentry, err error) {
	if dsn == "" {
		return
	}

	uri, err := url.Parse(dsn)
	if err != nil {
		return
	}
	if uri.User == nil {
		err = utils.Errorf("Sentry: dsn missing user")
		return
	}
	publicKey := uri.User.Username()
	secretKey, found := uri.User.Password()
	if !found {
		utils.Errorf("Sentry: dsn missing secret")
		return
	}

	sentry := &Sentry{}

	if idx := strings.LastIndex(uri.Path, "/"); idx != -1 {
		sentry.projectId = uri.Path[idx+1:]
		uri.Path = uri.Path[:idx+1] + "api/" + sentry.projectId + "/store/"
	}
	if sentry.projectId == "" {
		err = utils.Errorf("Sentry: dsn missing project id")
		return
	}

	sentry.url = uri.String()

	sentry.authHeader = fmt.Sprintf("Sentry sentry_version=4, sentry_key=%s, sentry_secret=%s", publicKey, secretKey)

	return
}

// TODO: Have something less general than interface here

func (self *Sentry) send(body *Packet) (err error) {
	buf := new(bytes.Buffer)
	if body != nil {
		if err = json.NewEncoder(buf).Encode(body); err != nil {
			return
		}
	}

	request, _ := http.NewRequest("POST", self.url, buf)
	request.Header.Set("X-Sentry-Auth", self.authHeader)
	request.Header.Set("Content-Type", "application/json")

	response, err := self.client.Do(request)
	defer response.Body.Close()
	if err != nil {
		return err
	}

	if response.StatusCode != 200 {
		return utils.Errorf("Sentry: sent request %v and received response %v", utils.Prettify(request), utils.Prettify(response))
	}

	return
}

/*
Sends error to Sentry
*/
func (self *Sentry) CaptureError(serr error, tags Tags) (err error) {
	packet := &Packet{}
	if err = packet.Init(); err != nil {
		return
	}

	if err = self.send(packet); err != nil {
		return
	}

	return
}

func (self *Packet) Init() (err error) {
	if self.EventId, err = uuid(); err != nil {
		return
	}
	self.Timestamp = time.Now()
	if self.Message == "" {
		return utils.Errorf("Sentry: Packet.Message missing")
	}
	if self.Level == "" {
		self.Level = ERROR
	}
	if self.Level != DEBUG &&
		self.Level != INFO &&
		self.Level != WARNING &&
		self.Level != ERROR &&
		self.Level != FATAL {
		return utils.Errorf("Sentry: Packet.Level value not valid")
	}
	if self.Logger == "" {
		self.Logger = "root"
	}
	/*
		// Optional
		Culprit    string `json:"culprit, omitempty"` // E.g. function name
		Tags       Tags   `json:"tags,omitempty"`
		ServerName string `json:"server_name,omitempty"`
	*/
	return
}

func uuid() (string, error) {
	id := make([]byte, 16)
	_, err := io.ReadFull(rand.Reader, id)
	if err != nil {
		return "", err
	}
	id[6] &= 0x0F // clear version
	id[6] |= 0x40 // set version to 4 (random uuid)
	id[8] &= 0x3F // clear variant
	id[8] |= 0x80 // set to IETF variant
	return hex.EncodeToString(id), nil
}
