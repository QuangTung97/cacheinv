package config

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMySQLDSN(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		conf := MySQLConfig{
			Host:     "domain1",
			Port:     1234,
			Username: "user1",
			Password: "pass1",
			Database: "db1",
			Options:  "parseTime=true",
		}
		assert.Equal(t, "user1:pass1@tcp(domain1:1234)/db1?parseTime=true", conf.DSN())
		assert.Equal(t, "user1:[SECRET]@tcp(domain1:1234)/db1?parseTime=true", conf.PrintDSN())
		assert.Equal(t, "user1:pass1@tcp(domain1:1234)/db1?parseTime=true", conf.DSN())
	})

	t.Run("with pass url escaped", func(t *testing.T) {
		conf := MySQLConfig{
			Host:     "domain1",
			Port:     1234,
			Username: "user1",
			Password: "@? A",
			Database: "db1",
			Options:  "parseTime=true",
		}
		assert.Equal(t, "user1:@%3F%20A@tcp(domain1:1234)/db1?parseTime=true", conf.DSN())
	})
}

func TestCopyConfigFile(_ *testing.T) {
	err := exec.Command("cp", "./config.yml", "../config.tmp.yml").Run()
	if err != nil {
		panic(err)
	}
}

func TestLoadConfig(t *testing.T) {
	conf := Load()
	assert.Equal(t, Config{
		HTTPPort: 11080,

		EventTableName:  "invalidate_events",
		OffsetTableName: "invalidate_offsets",

		EventRetentionSize: 10_000_000,
		NotifyAccessToken:  "",

		DBType: DBTypeMySQL,
		MySQL: MySQLConfig{
			Host:         "localhost",
			Port:         3306,
			Username:     "root",
			Password:     "1",
			Database:     "cache_inv",
			Options:      "parseTime=true",
			MaxOpenConns: 10,
			MaxIdleConns: 5,
		},

		ClientType:      ClientTypeRedis,
		RedisNumServers: 2,
		RedisServers: []RedisConfig{
			{
				ID:   11,
				Addr: "localhost:6379",
			},
			{
				ID:   12,
				Addr: "localhost:6380",
			},
		},
	}, conf)
}
