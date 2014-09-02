package email

import (
	"github.com/soundtrackyourbrand/utils/key"
)

type Attachment struct {
	ContentID string
	Name      string
	Data      []byte
}

type MailType string

type EmailParameters struct {
	To           string
	Cc           string
	Bcc          string
	Sender       string
	Attachments  []Attachment
	Locale       string
	TemplateName MailType
	MailContext  map[string]interface{}
}

type EmailTemplateSender interface {
	SendEmailTemplate(ep *EmailParameters, accountId *key.Key) error
}
