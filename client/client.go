package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

type ServiceConnector interface {
	GetAuthService() string
	GetRadioService() string
	GetPaymentService() string
	Client() *http.Client
}

type DefaultMeta struct {
	Id        key.Key        `json:"id,omitempty"`
	CreatedAt utils.JSONTime `json:"iso8601_created_at,omitempty"`
	UpdatedAt utils.JSONTime `json:"iso8601_updated_at,omitempty"`
	CreatedBy key.Key        `json:"created_by,omitempty"`
	UpdatedBy key.Key        `json:"updated_by,omitempty"`
}

type RemoteLocation struct {
	DefaultMeta

	Account key.Key `json:"account"`

	Name       string `json:"name"`
	PostalCode string `json:"postal_code"`
	Address    string `json:"address"`
	City       string `json:"city"`
	ISOCountry string `json:"iso_country"`
	Locale     string `json:"locale"`

	Deactivated bool `json:"deactivated" PUT_scopes:"Location_privileged" POST_scopes:"Account_privileged"`
}

type RemoteUser struct {
	DefaultMeta
	Name            string `json:"name,omitempty"`
	Email           string `json:"email,omitempty"`
	Locale          string `json:"locale,omitempty"`
	Password        string `json:"password,omitempty"`
	Backoffice      bool   `json:"backoffice,omitempty"`
	FreshdeskAPIKey string `json:"freshdesk_api_key,omitempty"`
}

func (self *RemoteUser) SendEmailTemplate(sender utils.EmailTemplateSender, cc string, mailContext map[string]interface{}, templateName utils.MailType, attachments []utils.Attachment, accountId *key.Key) error {
	return sender.SendEmailTemplate(self.Email, cc, mailContext, templateName, self.Locale, attachments, accountId)
}

type SoundZoneSettings struct {
	Mono    bool `json:"mono"`
	Offline bool `json:"offline"`
}

type ScheduleSettings struct {
	TrackSeparation  int `json:"track_separation"`
	ArtistSeparation int `json:"artist_separation"`
	AlbumSeparation  int `json:"album_separation"`
}

type RemoteProductQueue struct {
	DefaultMeta
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Queue       []string `json:"queue"`
}

type RemoteVoucher struct {
	DefaultMeta
	Code                     string         `json:"code"`
	Label                    string         `json:"label"`
	ValidUntil               utils.JSONTime `json:"iso8601_valid_until"`
	ProductQueue             key.Key        `json:"product_queue"`
	Email                    string         `json:"email"`
	MaxAccounts              int            `json:"max_accounts"`
	MaxActivationsPerAccount int            `json:"max_activations_per_account"`

	DenormProductQueue *RemoteProductQueue `json:"denorm_product_queue,omitempty"`
}

type RemotePaymentMethod struct {
	DefaultMeta
	ValidUntil    utils.JSONTime `json:"iso8601_valid_until"`
	MaskedCC      string         `json:"masked_cc"`
	PaymentMethod string         `json:"payment_method"`
	Voucher       string         `json:"voucher"`
	DenormVoucher *RemoteVoucher `json:"denorm_voucher,omitempty"`
}

type RemoteAccount struct {
	DefaultMeta
	Address               string           `json:"address,omitempty"`
	BusinessName          string           `json:"business_name,omitempty"`
	BusinessType          string           `json:"business_type,omitempty"`
	City                  string           `json:"city,omitempty"`
	Comment               string           `json:"comment,omitempty"`
	KeyAccountManager     string           `json:"key_account_manager,omitempty"`
	ISOCountry            string           `json:"iso_country,omitempty"`
	VATCode               string           `json:"vat_code,omitempty"`
	Locale                string           `json:"locale,omitempty"`
	Phone                 string           `json:"phone,omitempty"`
	PostalCode            string           `json:"postal_code,omitempty"`
	MaxSoundZones         int              `json:"max_sound_zones,omitempty"`
	MaxUnbilledSoundZones int              `json:"max_unbilled_sound_zones,omitempty"`
	Deactivated           bool             `json:"deactivated,omitempty"`
	OrgNumber             string           `json:"org_number,omitempty"`
	ScheduleSettings      ScheduleSettings `json:"schedule_settings,omitempty"`
	TrackSeparation       int              `json:"track_separation,omitempty"`
	ArtistSeparation      int              `json:"artist_separation,omitempty"`
	AlbumSeparation       int              `json:"album_separation,omitempty"`
}

type RemoteSoundZone struct {
	DefaultMeta
	Account                   key.Key        `json:"account,omitempty"`
	Address                   string         `json:"address,omitempty"`
	City                      string         `json:"city,omitempty"`
	Comment                   string         `json:"comment,omitempty"`
	Email                     string         `json:"email,omitempty"`
	ISOCountry                string         `json:"iso_country,omitempty"`
	Name                      string         `json:"name,omitempty,omitempty"`
	PostalCode                string         `json:"postal_code,omitempty"`
	Serial                    string         `json:"serial,omitempty"`
	SpotifyUsername           string         `json:"spotify_username,omitempty"`
	SpotifyPassword           string         `json:"spotify_password,omitempty"`
	PaidUntil                 utils.JSONTime `json:"iso8601_paid_until"`
	BilledUntil               utils.JSONTime `json:"iso8601_billed_until,omitempty"`
	Locale                    string         `json:"locale,omitempty"`
	Schedule                  key.Key        `json:"schedule,omitempty"`
	Deactivated               bool           `json:"deactivated"`
	SpotifyAccountDeactivated bool           `json:"spotify_account_deactivated"`
	DeviceId                  string         `json:"device_id,omitempty"`
}

type RemoteSoundZoneErrorRequest struct {
	Unique         bool                  `json:"unique"`
	SoundZoneError *RemoteSoundZoneError `json:"sound_zone_error"`
}

type RemoteSoundZoneError struct {
	DefaultMeta
	Type     string           `json:"type"`
	Cause    utils.ByteString `json:"cause"`
	Info     string           `json:"info"`
	Resolved bool             `json:"resolved"`
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
	ProductCode        string         `json:"current_product_code"`
	IsRecurring        bool           `json:"is_recurring"`
	LastAutoPayFailure bool           `json:"last_auto_pay_failure"`
	Deactivated        bool           `json:"deactivated"`
	Username           string         `json:"username"`
	Account            key.Key        `json:"account" datastore:"-"`
	ISOCountry         string         `json:"iso_country"`
}

func (self *RemoteSoundZone) SendEmailTemplate(sender utils.EmailTemplateSender, cc string, mailContext map[string]interface{}, templateName utils.MailType, attachments []utils.Attachment) error {
	accountId := self.Id.Parent().Parent()
	return sender.SendEmailTemplate(self.Email, cc, mailContext, templateName, self.Locale, attachments, &accountId)
}

func errorFor(request *http.Request, response *http.Response) (err error) {
	var b []byte
	if b, err = ioutil.ReadAll(response.Body); err != nil {
		return
	}
	err = jsoncontext.NewError(response.StatusCode, string(b), fmt.Sprintf("Got %+v when doing %+v\n%v", response, request, string(b)), nil)
	return
}

func DoRequest(c ServiceConnector, method, service, path string, token AccessToken, body interface{}) (request *http.Request, response *http.Response, err error) {
	buf := new(bytes.Buffer)
	if body != nil {
		if err = json.NewEncoder(buf).Encode(body); err != nil {
			return
		}
	}

	request, err = http.NewRequest(method, fmt.Sprintf("%v/%v", service, path), buf)
	if err != nil {
		return
	}

	if token != nil {
		var encoded string
		encoded, err = token.EncodeSelf()
		if err != nil {
			return
		}
		request.Header.Add("Authorization", fmt.Sprintf("Bearer %v", encoded))
	}

	if method == "POST" || method == "PUT" {
		request.Header.Add("Content-Type", "application/json")
	}

	//TODO, we should start using version 2!
	request.Header.Add("X-API-Version", "1")
	response, err = c.Client().Do(request)
	if err != nil {
		return
	}
	newBody := &bytes.Buffer{}
	if _, err = io.Copy(newBody, response.Body); err != nil {
		return
	}
	if err = response.Body.Close(); err != nil {
		return
	}
	response.Body = ioutil.NopCloser(newBody)
	return
}

type CountContainer struct {
	Count int `json:"count"`
}

func CountSoundZonesForSchedule(c ServiceConnector, schedule key.Key, token AccessToken) (result int, err error) {
	request, response, err := DoRequest(c, "GET", c.GetAuthService(), fmt.Sprintf("schedules/%v/sound_zone_count", schedule.Encode()), token, nil)
	if err != nil {
		return
	}
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	container := &CountContainer{}
	if err = json.NewDecoder(response.Body).Decode(container); err != nil {
		return
	}

	result = container.Count

	return
}

func GetLocation(c ServiceConnector, location key.Key, token AccessToken) (result *RemoteLocation, err error) {
	request, response, err := DoRequest(c, "GET", c.GetAuthService(), fmt.Sprintf("locations/%v", location.Encode()), token, nil)
	if err != nil {
		return
	}
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteLocation{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}

func GetAccountContact(c ServiceConnector, account key.Key, token AccessToken) (result *RemoteUser, err error) {
	request, response, err := DoRequest(c, "GET", c.GetAuthService(), fmt.Sprintf("accounts/%v/contact", account.Encode()), token, nil)
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

func GetUser(c ServiceConnector, user key.Key, token AccessToken) (result *RemoteUser, err error) {
	request, response, err := DoRequest(c, "GET", c.GetAuthService(), fmt.Sprintf("users/%v", user.Encode()), token, nil)
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
	request, response, err := DoRequest(c, "POST", c.GetRadioService(), fmt.Sprintf("schedules/%v/slots", slot.Schedule.Encode()), token, slot)
	if response.StatusCode != 201 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteSlot{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}

func CreateUser(c ServiceConnector, user RemoteUser) (result *RemoteUser, err error) {
	request, response, err := DoRequest(c, "POST", c.GetAuthService(), "users", nil, user)
	if response.StatusCode != 201 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteUser{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}
func UpdateUser(c ServiceConnector, user *RemoteUser, token AccessToken) (result *RemoteUser, err error) {
	request, response, err := DoRequest(c, "PUT", c.GetAuthService(), fmt.Sprintf("users/%v", user.Id.Encode()), token, user)
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteUser{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}

func Auth(c ServiceConnector, auth_request AuthRequest) (result *DefaultAccessToken, encoded string, err error) {
	request, response, err := DoRequest(c, "POST", c.GetAuthService(), "auth", nil, auth_request)
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

func GetPaymentMethodByAccountId(c ServiceConnector, account key.Key, token AccessToken) (result *RemotePaymentMethod, err error) {
	request, response, err := DoRequest(c, "GET", c.GetPaymentService(), fmt.Sprintf("accounts/%v/payment_method", account.Encode()), token, nil)
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &RemotePaymentMethod{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}

func GetAccount(c ServiceConnector, account key.Key, token AccessToken) (result *RemoteAccount, err error) {
	request, response, err := DoRequest(c, "GET", c.GetAuthService(), fmt.Sprintf("accounts/%v", account.Encode()), token, nil)
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteAccount{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}

func GetAccounts(c ServiceConnector, user key.Key, token AccessToken) (result []RemoteAccount, err error) {
	request, response, err := DoRequest(c, "GET", c.GetAuthService(), fmt.Sprintf("users/%v/accounts", user.Encode()), token, nil)
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = []RemoteAccount{}
	err = json.NewDecoder(response.Body).Decode(&result)
	return
}

func GetTelemarketingDropoutAccounts(c ServiceConnector, token AccessToken) (result []RemoteAccount, err error) {
	request, response, err := DoRequest(c, "GET", c.GetAuthService(), "telemarketing_dropout_accounts", token, nil)
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = []RemoteAccount{}
	err = json.NewDecoder(response.Body).Decode(&result)
	return
}

func CreateSoundZone(c ServiceConnector, token AccessToken, remoteSoundZone RemoteSoundZone) (result *RemoteSoundZone, err error) {
	request, response, err := DoRequest(c, "POST", c.GetAuthService(), fmt.Sprintf("accounts/%v/soundzones", remoteSoundZone.Account.Encode()), token, remoteSoundZone)
	if response.StatusCode != 201 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteSoundZone{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}

func UpdateSoundZone(c ServiceConnector, token AccessToken, updatedSoundZone RemoteSoundZone) (err error) {
	request, response, err := DoRequest(c, "PUT", c.GetAuthService(), fmt.Sprintf("soundzones/%v", updatedSoundZone.Id.Encode()), token, updatedSoundZone)
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	return
}

func UpdateSoundZoneErrors(c ServiceConnector, token AccessToken, soundZoneId key.Key, soundZoneErrorReq RemoteSoundZoneErrorRequest) (err error) {
	request, response, err := DoRequest(c, "POST", c.GetAuthService(), fmt.Sprintf("sound_zones/%v/sound_zone_errors", soundZoneId.Encode()), token, soundZoneErrorReq)
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	return
}

func CreateAccount(c ServiceConnector, token AccessToken, account RemoteAccount, owner key.Key) (result *RemoteAccount, err error) {
	request, response, err := DoRequest(c, "POST", c.GetAuthService(), fmt.Sprintf("users/%v/accounts", owner.Encode()), token, account)
	if response.StatusCode != 201 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteAccount{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}

func GetSoundZone(c ServiceConnector, soundZone key.Key, token AccessToken) (result *RemoteSoundZone, err error) {
	request, response, err := DoRequest(c, "GET", c.GetAuthService(), fmt.Sprintf("soundzones/%v", soundZone.Encode()), token, nil)
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteSoundZone{}
	err = json.NewDecoder(response.Body).Decode(result)
	return
}

func GetSoundZones(c ServiceConnector, account_id key.Key, token AccessToken) (result []RemoteSoundZone, err error) {
	request, response, err := DoRequest(c, "GET", c.GetAuthService(), fmt.Sprintf("accounts/%v/soundzones", account_id.Encode()), token, nil)
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = []RemoteSoundZone{}
	err = json.NewDecoder(response.Body).Decode(&result)
	return
}

func GetSpotifyAccount(c ServiceConnector, soundZone key.Key, token AccessToken) (result *RemoteSpotifyAccount, err error) {
	request, response, err := DoRequest(c, "GET", c.GetPaymentService(), fmt.Sprintf("soundzones/%v/spotify_account", soundZone.Encode()), token, nil)
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteSpotifyAccount{}
	err = json.NewDecoder(response.Body).Decode(result)
	return
}

func SetPassword(c ServiceConnector, user key.Key, password string, token AccessToken) (result *RemoteUser, err error) {
	request, response, err := DoRequest(c, "PUT", c.GetAuthService(), fmt.Sprintf("users/%s/password", user.Encode()), token, map[string]string{
		"password": password,
	})
	if response.StatusCode != 200 {
		err = errorFor(request, response)
		return
	}

	result = &RemoteUser{}
	err = json.NewDecoder(response.Body).Decode(result)

	return
}
