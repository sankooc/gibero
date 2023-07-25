package gibero

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/pem"
	"errors"
)

func DecryptWithPrivateKey(ciphertext []byte, priv *rsa.PrivateKey) ([]byte, error) {
	hash := sha1.New()
	plaintext, err := rsa.DecryptOAEP(hash, rand.Reader, priv, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}

func BytesToPrivateKey(raw []byte, password []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(raw)
	enc := x509.IsEncryptedPEMBlock(block)
	b := block.Bytes
	var err error
	if enc {
		b, err = x509.DecryptPEMBlock(block, password)
		if err != nil {
			return nil, err
		}
	}
	val, err := x509.ParsePKCS8PrivateKey(b)
	if err != nil {
		return nil, err
	}
	key, ok := val.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("known_key")
	}
	return key, nil
}

func BytesToPublicKey(raw []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(raw)
	publicKeyInterface, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	return publicKeyInterface.(*rsa.PublicKey), nil
}

func EncryptWithPublicKey(plainText []byte, publickey *rsa.PublicKey) []byte {
	sha1 := sha1.New()
	cipherText, err := rsa.EncryptOAEP(sha1, rand.Reader, publickey, plainText, nil)
	if err != nil {
		panic(err)
	}
	return cipherText
}
