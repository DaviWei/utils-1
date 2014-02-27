package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/soundtrackyourbrand/utils"
	"github.com/soundtrackyourbrand/utils/key"
	"github.com/soundtrackyourbrand/utils/web/jsoncontext"
)

type DefaultAccessToken struct {
	Principal     key.Key   `json:"principal" jsonTo:"string"`
	PrincipalKind string    `json:"principal_kind"`
	Parent        key.Key   `json:"parent" jsonTo:"string"`
	ParentKind    string    `json:"parent_kind"`
	Credentials   []byte    `json:"credentials" jsonTo:"string"`
	IssuedAt      time.Time `json:"issued_at"`
	AccessScopes  []string  `json:"access_scopes"`
	IsAuthorized  bool      `json:"is_authorized"`
	Resource      key.Key   `json:"resource,omitempty" jsonTo:"string"`
	UID           int64     `json:"uid"`
	Encoded       string    `json:"encoded"`
}

func (self DefaultAccessToken) EncodeSelf() (string, error) {
	return self.Encoded, nil
}

type AuthRequest struct {
	Username  string   `json:"username,omitempty"`
	GrantType string   `json:"grant_type,omitempty"`
	Password  string   `json:"password,omitempty"`
	Scopes    []string `json:"scopes"`
	Resource  key.Key  `json:"resource,omitempty" jsonTo:"string"`
	Authorize bool     `json:"authorize"`
}

type AccessToken interface {
	EncodeSelf() (string, error)
}

type ServiceConfig interface {
	AuthService() string
	RadioService() string
	PaymentService() string
}

type ServiceConnector interface {
	ServiceConfig
	Client() *http.Client
}

type DefaultMeta struct {
	Id        key.Key        `json:"id,omitempty"`
	CreatedAt utils.JSONTime `json:"iso8601_created_at,omitempty"`
	UpdatedAt utils.JSONTime `json:"iso8601_created_at,omitempty"`
}

type RemoteUser struct {
	DefaultMeta
	Name            string `json:"name,omitempty"`
	Email           string `json:"email,omitempty"`
	Locale          string `json:"locale,omitempty"`
	Password        string `json:"password,omitempty"`
	Admin           bool   `json:"admin,omitempty"`
	FreshdeskAPIKey string `json:"freshdesk_api_key,omitempty"`
}

func (self *RemoteUser) SendEmailTemplate(sender utils.EmailTemplateSender, mailContext map[string]interface{}, templateName string, attachments []utils.Attachment) error {
	return sender.SendEmailTemplate(self.Email, mailContext, templateName, self.Locale, attachments)
}

type RemoteAccount struct {
	DefaultMeta
	Address       string  `json:"address,omitempty"`
	BusinessName  string  `json:"business_name,omitempty"`
	BusinessType  string  `json:"business_type,omitempty"`
	City          string  `json:"city,omitempty"`
	Comment       string  `json:"comment,omitempty"`
	ISOCountry    string  `json:"iso_country,omitempty"`
	VATCode       string  `json:"vat_code,omitempty"`
	Locale        string  `json:"locale,omitempty"`
	Phone         string  `json:"phone,omitempty"`
	AdminUser     key.Key `json:"admin_user,omitempty"`
	PostalCode    string  `json:"postal_code,omitempty"`
	MaxSoundZones int     `json:"max_sound_zones,omitempty"`
	Deactivated   bool    `json:"deactivated,omitempty"`
	OrgNumber     string  `json:"org_number,omitempty"`
}

type RemoteSoundZone struct {
	DefaultMeta
	Account         key.Key        `json:"account,omitempty"`
	Address         string         `json:"address,omitempty"`
	City            string         `json:"city,omitempty"`
	Comment         string         `json:"comment,omitempty"`
	Email           string         `json:"email,omitempty"`
	ISOCountry      string         `json:"iso_country,omitempty"`
	Name            string         `json:"name,omitempty,omitempty"`
	PostalCode      string         `json:"postal_code,omitempty"`
	Serial          string         `json:"serial,omitempty"`
	SpotifyUsername string         `json:"spotify_username,omitempty"`
	SpotifyPassword string         `json:"spotify_password,omitempty"`
	PaidUntil       utils.JSONTime `json:"iso8601_paid_until"`
	Locale          string         `json:"locale,omitempty"`
	Schedule        key.Key        `json:"schedule"`
}

type RemoteSlot struct {
	DefaultMeta

	DTSTART  string  `json:"DTSTART"`
	DURATION string  `json:"DURATION"`
	RRULE    string  `json:"RRULE"`
	Type     string  `json:"type"`
	Source   string  `json:"source"`
	Schedule key.Key `json:"-"`
}

type RemoteSpotifyAccount struct {
	DefaultMeta
	SoundZone          key.Key        `json:"sound_zone" datastore:"-"`
	PaidUntil          utils.JSONTime `json:"iso8601_paid_until"`
	ProductCode        string         `json:"product_code"`
	IsRecurring        bool           `json:"is_recurring"`
	LastAutoPayFailure bool           `json:"last_auto_pay_failure"`
	Deactivated        bool           `json:"deactivated"`
	Username           string         `json:"username"`
	Account            key.Key        `json:"account" datastore:"-"`
}

func (self *RemoteSoundZone) SendEmailTemplate(sender utils.EmailTemplateSender, mailContext map[string]interface{}, templateName string, attachments []utils.Attachment) error {
	return sender.SendEmailTemplate(self.Email, mailContext, templateName, self.Locale, attachments)
}

func errorFor(request *http.Request, response *http.Response) (err error) {
	var b []byte
	if b, err = ioutil.ReadAll(response.Body); err != nil {
		return
	}
	err = jsoncontext.NewError(response.StatusCode, string(b), fmt.Sprintf("Got %+v when doing %+v\n%v", response, request, string(b)), nil)
	return
}

func GetUser(c ServiceConnector, user key.Key, token AccessToken) (result *RemoteUser, err error) {
	var request *http.Request
	request, err = http.NewRequest("GET", fmt.Sprintf("%v/users/%v", c.AuthService(), user.Encode()), nil)
	if err != nil {
		return
	}

	encoded, err := token.EncodeSelf()
	if err != nil {
		return
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %v", encoded))
	request.Header.Add("X-API-Version", "1")

	var response *http.Response
	response, err = c.Client().Do(request)
	if err != nil {
		return
	}
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteUser{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}

func CreateSlot(c ServiceConnector, token AccessToken, slot RemoteSlot) (result *RemoteSlot, err error) {
	var request *http.Request
	buf := new(bytes.Buffer)
	err = json.NewEncoder(buf).Encode(slot)
	if err != nil {
		return
	}
	request, err = http.NewRequest("POST", fmt.Sprintf("%v/schedules/%v/slots", c.RadioService(), slot.Schedule.Encode()), buf)
	if err != nil {
		return
	}
	encoded, err := token.EncodeSelf()
	if err != nil {
		return
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %v", encoded))
	request.Header.Add("X-API-Version", "1")
	request.Header.Add("Content-Type", "application/json")

	var response *http.Response

	response, err = c.Client().Do(request)
	if err != nil {
		return
	}
	if response.StatusCode != 201 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteSlot{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}

func CreateUser(c ServiceConnector, user RemoteUser) (result *RemoteUser, err error) {
	var request *http.Request
	buf := new(bytes.Buffer)
	err = json.NewEncoder(buf).Encode(user)
	if err != nil {
		return
	}
	request, err = http.NewRequest("POST", fmt.Sprintf("%v/users", c.AuthService()), buf)
	if err != nil {
		return
	}
	request.Header.Add("X-API-Version", "1")
	request.Header.Add("Content-Type", "application/json")

	var response *http.Response

	response, err = c.Client().Do(request)
	if err != nil {
		return
	}
	if response.StatusCode != 201 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteUser{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}
func UpdateUser(c ServiceConnector, user RemoteUser, token AccessToken) (result *RemoteUser, err error) {
	var request *http.Request
	buf := new(bytes.Buffer)
	err = json.NewEncoder(buf).Encode(user)
	if err != nil {
		return
	}
	request, err = http.NewRequest("PUT", fmt.Sprintf("%v/users/%v", c.AuthService(), user.Id.Encode()), buf)
	if err != nil {
		return
	}

	encoded, err := token.EncodeSelf()
	if err != nil {
		return
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %v", encoded))
	request.Header.Add("X-API-Version", "1")
	request.Header.Add("Content-Type", "application/json")

	var response *http.Response

	response, err = c.Client().Do(request)
	if err != nil {
		return
	}
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteUser{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}

func Auth(c ServiceConnector, auth_request AuthRequest) (result *DefaultAccessToken, encoded string, err error) {
	var request *http.Request
	buf := new(bytes.Buffer)
	err = json.NewEncoder(buf).Encode(auth_request)
	if err != nil {
		return
	}
	request, err = http.NewRequest("POST", fmt.Sprintf("%v/auth", c.AuthService()), buf)
	if err != nil {
		return
	}
	request.Header.Add("X-API-Version", "1")
	request.Header.Add("Content-Type", "application/json")

	var response *http.Response

	response, err = c.Client().Do(request)
	if err != nil {
		return
	}
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &DefaultAccessToken{}
	err = json.NewDecoder(response.Body).Decode(result)
	result.Encoded = strings.Join(response.Header["X-Access-Token-Issued"], "")
	result.Encoded = strings.Replace(result.Encoded, ",", "", -1)
	result.Encoded = strings.Replace(result.Encoded, " ", "", -1)
	return
}

func GetAccount(c ServiceConnector, account key.Key, token AccessToken) (result *RemoteAccount, err error) {
	var request *http.Request
	request, err = http.NewRequest("GET", fmt.Sprintf("%v/accounts/%v", c.AuthService(), account.Encode()), nil)
	if err != nil {
		return
	}
	encoded, err := token.EncodeSelf()
	if err != nil {
		return
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %v", encoded))
	request.Header.Add("X-API-Version", "1")

	var response *http.Response
	response, err = c.Client().Do(request)
	if err != nil {
		return
	}
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteAccount{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}

func GetAccounts(c ServiceConnector, user key.Key, token AccessToken) (result *[]RemoteAccount, err error) {
	var request *http.Request
	request, err = http.NewRequest("GET", fmt.Sprintf("%v/users/%v/accounts", c.AuthService(), user.Encode()), nil)
	if err != nil {
		return
	}
	encoded, err := token.EncodeSelf()
	if err != nil {
		return
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %v", encoded))
	request.Header.Add("X-API-Version", "1")

	var response *http.Response
	response, err = c.Client().Do(request)
	if err != nil {
		return
	}
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &[]RemoteAccount{}
	err = json.NewDecoder(response.Body).Decode(result)
	return
}

func CreateSoundZone(c ServiceConnector, token AccessToken, remoteSoundZone RemoteSoundZone) (result *RemoteSoundZone, err error) {
	// Read body
	buf := new(bytes.Buffer)
	err = json.NewEncoder(buf).Encode(remoteSoundZone)
	if err != nil {
		return
	}

	// Create request
	var request *http.Request
	request, err = http.NewRequest("POST", fmt.Sprintf("%v/accounts/%v/soundzones", c.AuthService(), remoteSoundZone.Account.Encode()), buf)
	if err != nil {
		return
	}

	encoded, err := token.EncodeSelf()
	if err != nil {
		return
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %v", encoded))
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("X-API-Version", "1")

	var response *http.Response
	response, err = c.Client().Do(request)
	if err != nil {
		return
	}
	if response.StatusCode != 201 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteSoundZone{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}

func UpdateSoundZone(c ServiceConnector, token AccessToken, updatedSoundZone RemoteSoundZone) (err error) {
	// Read body
	buf := new(bytes.Buffer)
	err = json.NewEncoder(buf).Encode(updatedSoundZone)
	if err != nil {
		return
	}

	// Create request
	var request *http.Request
	request, err = http.NewRequest("PUT", fmt.Sprintf("%v/soundzones/%v", c.AuthService(), updatedSoundZone.Id.Encode()), buf)
	if err != nil {
		return
	}

	encoded, err := token.EncodeSelf()
	if err != nil {
		return
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %v", encoded))
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("X-API-Version", "1")

	var response *http.Response
	response, err = c.Client().Do(request)
	if err != nil {
		return
	}
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	return
}

func CreateAccount(c ServiceConnector, token AccessToken, account RemoteAccount) (result *RemoteAccount, err error) {
	// Read body
	buf := new(bytes.Buffer)
	err = json.NewEncoder(buf).Encode(account)
	if err != nil {
		return
	}

	// Create request
	var request *http.Request
	request, err = http.NewRequest("POST", fmt.Sprintf("%v/users/%v/accounts", c.AuthService(), account.AdminUser.Encode()), buf)
	if err != nil {
		return
	}

	encoded, err := token.EncodeSelf()
	if err != nil {
		return
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %v", encoded))
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("X-API-Version", "1")

	var response *http.Response
	response, err = c.Client().Do(request)
	if err != nil {
		return
	}
	if response.StatusCode != 201 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteAccount{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}

func GetSoundZone(c ServiceConnector, soundZone key.Key, token AccessToken) (result *RemoteSoundZone, err error) {
	var request *http.Request
	request, err = http.NewRequest("GET", fmt.Sprintf("%v/soundzones/%v", c.AuthService(), soundZone.Encode()), nil)
	if err != nil {
		return
	}
	encoded, err := token.EncodeSelf()
	if err != nil {
		return
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %v", encoded))
	request.Header.Add("X-API-Version", "1")

	var response *http.Response
	response, err = c.Client().Do(request)
	if err != nil {
		return
	}
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteSoundZone{}
	err = json.NewDecoder(response.Body).Decode(result)
	return
}

func GetSoundZones(c ServiceConnector, account_id key.Key, token AccessToken) (result *[]RemoteSoundZone, err error) {
	var request *http.Request
	request, err = http.NewRequest("GET", fmt.Sprintf("%v/accounts/%v/soundzones", c.AuthService(), account_id.Encode()), nil)
	if err != nil {
		return
	}
	encoded, err := token.EncodeSelf()
	if err != nil {
		return
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %v", encoded))
	request.Header.Add("X-API-Version", "1")

	var response *http.Response
	response, err = c.Client().Do(request)
	if err != nil {
		return
	}
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &[]RemoteSoundZone{}
	err = json.NewDecoder(response.Body).Decode(result)
	return
}

func GetSpotifyAccount(c ServiceConnector, soundZone key.Key, token AccessToken) (result *RemoteSpotifyAccount, err error) {
	var request *http.Request
	request, err = http.NewRequest("GET", fmt.Sprintf("%v/soundzones/%v/spotify_account", c.PaymentService(), soundZone.Encode()), nil)
	if err != nil {
		return
	}
	encoded, err := token.EncodeSelf()
	if err != nil {
		return
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %v", encoded))
	request.Header.Add("X-API-Version", "1")

	var response *http.Response
	response, err = c.Client().Do(request)
	if err != nil {
		return
	}
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteSpotifyAccount{}
	err = json.NewDecoder(response.Body).Decode(result)
	return
}

func SetPassword(c ServiceConnector, user key.Key, password string, token AccessToken) (result *RemoteUser, err error) {

	body := new(bytes.Buffer)
	err = json.NewEncoder(body).Encode(map[string]string{
		"password":  password,
	})
	if err != nil {
		return
	}

	// Create request
	var request *http.Request
	request, err = http.NewRequest("PUT", fmt.Sprintf("%s/users/%s/password", c.AuthService(), user.Encode()), body)
	if err != nil {
		return
	}

	encoded, err := token.EncodeSelf()
	if err != nil {
		return
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %v", encoded))
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("X-API-Version", "1")

	var response *http.Response
	response, err = c.Client().Do(request)
	if err != nil {
		return
	}
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteUser{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}


