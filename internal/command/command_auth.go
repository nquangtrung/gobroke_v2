package command

import "github.com/golang-jwt/jwt/v5"

type AuthCommand struct {
	BaseCommand
	token  string
	claims jwt.MapClaims
}

func (c *AuthCommand) Token() string {
	if len(c.params) > 0 {
		return c.params[0]
	}
	return ""
}

func (c *AuthCommand) Claims() jwt.MapClaims {
	return c.claims
}

func (c *AuthCommand) DecodeToken(secretKey []byte) error {
	token, err := jwt.Parse(c.Token(), func(token *jwt.Token) (interface{}, error) {
		return secretKey, nil
	})
	if err != nil {
		return err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		c.claims = claims
	} else {
		return err
	}

	return nil
}

func NewAuthCommand(token string) Command {
	return &AuthCommand{
		BaseCommand: BaseCommand{
			action: Auth,
			params: []string{token},
		},
		token: token,
	}
}
