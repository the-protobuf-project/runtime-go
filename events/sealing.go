package events

// sealing.go keeps webhook signing secrets encrypted at rest.
//
// A signing secret is a credential: whoever reads it can forge deliveries a
// receiver will trust. So it is sealed with AES-256-GCM before the store keeps
// it, and opened only to sign. The key belongs in configuration, never in a
// column beside the secret.
//
// A sealed value reads "enc:v1:<key id>:<base64 nonce+ciphertext>". The key id
// is the first eight hex digits of the key's SHA-256, so a sealer holding the
// current key and the ones it replaced opens whichever sealed a value. A value
// without the prefix is a secret stored before sealing was turned on, and is
// used as it is until the webhook's next rotation seals it.

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// SealedPrefix starts every sealed secret.
const SealedPrefix = "enc:v1:"

// Sealer encrypts and decrypts signing secrets. A nil Sealer stores them as
// they are.
type Sealer struct {
	keys []sealKey // the first seals; all open
}

type sealKey struct {
	id   string
	aead cipher.AEAD
}

// NewSealer reads the current key and the keys it replaced, each the base64
// of 32 random bytes (openssl rand -base64 32). No current key is no sealer,
// and a nil one.
func NewSealer(current string, previous []string) (*Sealer, error) {
	if current == "" {
		if len(previous) > 0 {
			return nil, errors.New("events: previous secret keys are set but the current one is not; set the key that seals from now on")
		}
		return nil, nil //nolint:nilnil // no key is no sealer: a nil *Sealer stores secrets as they are
	}
	s := &Sealer{}
	for i, enc := range append([]string{current}, previous...) {
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(enc))
		if err != nil || len(raw) != 32 {
			what := "the secret key"
			if i > 0 {
				what = fmt.Sprintf("previous secret key %d", i-1)
			}
			return nil, fmt.Errorf("events: %s must be the base64 of 32 bytes (openssl rand -base64 32)", what)
		}
		block, err := aes.NewCipher(raw)
		if err != nil {
			return nil, err
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(raw)
		s.keys = append(s.keys, sealKey{id: hex.EncodeToString(sum[:4]), aead: aead})
	}
	return s, nil
}

// Seal encrypts secret under the current key.
func (s *Sealer) Seal(secret string) (string, error) {
	if s == nil || secret == "" {
		return secret, nil
	}
	k := s.keys[0]
	nonce := make([]byte, k.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	box := k.aead.Seal(nonce, nonce, []byte(secret), nil)
	return SealedPrefix + k.id + ":" + base64.StdEncoding.EncodeToString(box), nil
}

// Open decrypts a stored secret. One stored before sealing comes back as it
// is.
func (s *Sealer) Open(stored string) (string, error) {
	rest, ok := strings.CutPrefix(stored, SealedPrefix)
	if !ok {
		return stored, nil
	}
	id, enc, ok := strings.Cut(rest, ":")
	if !ok {
		return "", errors.New("the webhook's signing secret is malformed")
	}
	if s == nil {
		return "", errors.New("the webhook's signing secret is sealed, and this server has no secret key to open it")
	}
	box, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return "", errors.New("the webhook's signing secret is malformed")
	}
	for _, k := range s.keys {
		if k.id != id {
			continue
		}
		n := k.aead.NonceSize()
		if len(box) < n {
			return "", errors.New("the webhook's signing secret is malformed")
		}
		plain, err := k.aead.Open(nil, box[:n], box[n:], nil)
		if err != nil {
			return "", errors.New("the webhook's signing secret does not open under its key")
		}
		return string(plain), nil
	}
	return "", fmt.Errorf("the webhook's signing secret was sealed with key %s, which is neither the secret key nor a previous one", id)
}
