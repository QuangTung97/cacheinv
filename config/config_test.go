package config

import (
	"os/exec"
	"testing"
	"time"

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
		DBScanDuration:     30 * time.Second,

		NotifyAccessToken: "",

		DBType: DBTypeMySQL,
		MySQL: MySQLConfig{
			Host:     "localhost",
			Port:     3306,
			Username: "root",
			Password: "1",
			Database: "cache_inv",
			Options:  "parseTime=true",

			MaxOpenConns:    10,
			MaxIdleConns:    5,
			MaxConnIdleTime: 60 * time.Minute,
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

func TestValidateServerIDs(t *testing.T) {
	t.Run("duplicated", func(t *testing.T) {
		c := Config{
			RedisServers: []RedisConfig{
				{ID: 11, Addr: "localhost:6379"},
				{ID: 11, Addr: "localhost:6380"},
			},
		}
		assert.PanicsWithValue(t, "duplicated redis server id '11'", func() {
			c.validateRedisServers()
		})
	})

	t.Run("server id empty", func(t *testing.T) {
		c := Config{
			RedisServers: []RedisConfig{
				{ID: 0},
			},
		}
		assert.PanicsWithValue(t, "redis server id must not be empty", func() {
			c.validateRedisServers()
		})
	})

	t.Run("server addr empty", func(t *testing.T) {
		c := Config{
			RedisServers: []RedisConfig{
				{
					ID:   11,
					Addr: "",
				},
			},
		}
		assert.PanicsWithValue(t, "redis server address must not be empty", func() {
			c.validateRedisServers()
		})
	})

	t.Run("server addr duplicated", func(t *testing.T) {
		c := Config{
			RedisServers: []RedisConfig{
				{
					ID:   11,
					Addr: "addr1",
				},
				{
					ID:   12,
					Addr: "addr1",
				},
			},
		}
		assert.PanicsWithValue(t, "duplicated redis server address 'addr1'", func() {
			c.validateRedisServers()
		})
	})

	t.Run("redis servers is emtpy", func(t *testing.T) {
		c := Config{
			RedisServers: []RedisConfig{},
		}
		assert.PanicsWithValue(t, "redis server list must not be empty", func() {
			c.validateRedisServers()
		})
	})
}
