package ssh

import (
	"io"

	"github.com/soundtrackyourbrand/ssh"
)

func ParseCreds(user string, b []byte) (result Creds, err error) {
	k, err := ssh.ParsePrivateKey(b)
	if err != nil {
		return
	}
	result = Creds{
		keys: []ssh.Signer{k},
		user: user,
	}
	return
}

type Creds struct {
	keys []ssh.Signer
	user string
}

func (self Creds) Key(i int) (key ssh.PublicKey, err error) {
	if i < len(self.keys) {
		key = self.keys[i].PublicKey()
	}
	return
}

func (self Creds) Sign(i int, rand io.Reader, data []byte) (sig []byte, err error) {
	return self.keys[i].Sign(rand, data)
}

func New(creds Creds, addr string) (result *ssh.Session, err error) {
	sshConn, err := ssh.Dial("tcp", addr, &ssh.ClientConfig{
		User: creds.user,
		Auth: []ssh.ClientAuth{
			ssh.ClientAuthKeyring(creds),
		},
	})

	if err != nil {
		return
	}

	result, err = sshConn.NewSession()
	return
}
