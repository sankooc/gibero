package gibero

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDsn(t *testing.T) {
	assert := require.New(t)
	str := "tibero://username:password@127.0.0.1:1521/dbname?charset=urc"
	dsn := DsnConvert(str)
	assert.True(dsn != nil, "parseFaild")
	assert.Equal(dsn.username, "username", "username incorrect")
	assert.Equal(dsn.password, "password", "password incorrect")
	assert.Equal(dsn.address, "127.0.0.1:1521", "address incorrect")
	assert.Equal(dsn.dbname, "dbname", "dbname incorrect")
}
