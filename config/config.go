package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config ...
type Config struct {
	HTTPPort uint16 `mapstructure:"http_port"`

	EventTableName  string `mapstructure:"event_table_name"`
	OffsetTableName string `mapstructure:"offset_table_name"`

	EventRetentionSize uint32        `mapstructure:"event_retention_size"`
	DBScanDuration     time.Duration `mapstructure:"db_scan_duration"`

	NotifyAccessToken string `mapstructure:"notify_access_token"`

	DBType DBType      `mapstructure:"db_type"`
	MySQL  MySQLConfig `mapstructure:"mysql"`

	ClientType      ClientType    `mapstructure:"client_type"`
	RedisNumServers int           `mapstructure:"redis_num_servers"`
	RedisServers    []RedisConfig `mapstructure:"-"`
}

// DBType ...
type DBType string

const (
	// DBTypeMySQL ...
	DBTypeMySQL DBType = "mysql"
)

// ClientType ...
type ClientType string

const (
	// ClientTypeRedis ...
	ClientTypeRedis ClientType = "redis"
)

// MySQLConfig ...
type MySQLConfig struct {
	Host     string `mapstructure:"host"`
	Port     uint16 `mapstructure:"port"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
	Options  string `mapstructure:"options"`

	MaxOpenConns    uint32        `mapstructure:"max_open_conns"`
	MaxIdleConns    uint32        `mapstructure:"max_idle_conns"`
	MaxConnIdleTime time.Duration `mapstructure:"max_conn_idle_time"`
}

// RedisConfig ...
type RedisConfig struct {
	ID   uint32
	Addr string
}

// Load ...
func Load() Config {
	vip := viper.New()

	vip.SetConfigName("config")
	vip.SetConfigType("yml")
	vip.AddConfigPath(".")

	return loadConfigWithViper(vip)
}

func loadConfigWithViper(vip *viper.Viper) Config {
	vip.SetEnvPrefix("")
	vip.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	vip.AutomaticEnv()

	err := vip.ReadInConfig()
	if err != nil {
		panic(err)
	}

	// workaround https://github.com/spf13/viper/issues/188#issuecomment-399518663
	// to allow read from environment variables when Unmarshal
	for _, key := range vip.AllKeys() {
		val := vip.Get(key)
		vip.Set(key, val)
	}

	fmt.Println("Config file used:", vip.ConfigFileUsed())

	var cfg Config
	err = vip.Unmarshal(&cfg)
	if err != nil {
		panic(err)
	}

	for i := 0; i < cfg.RedisNumServers; i++ {
		key := fmt.Sprintf("redis_server_%d", i+1)

		idKey := key + "_id"
		serverID := vip.GetUint32(idKey)

		addrKey := key + "_addr"
		addr := vip.GetString(addrKey)

		if serverID == 0 {
			panic(fmt.Sprintf("missing config key '%s'", idKey))
		}
		if len(addr) == 0 {
			panic(fmt.Sprintf("missing config key '%s'", addrKey))
		}

		cfg.RedisServers = append(cfg.RedisServers, RedisConfig{
			ID:   serverID,
			Addr: addr,
		})
	}

	cfg.validateRedisServers()

	return cfg
}

// DSN ...
func (c MySQLConfig) DSN() string {
	pass := url.PathEscape(c.Password)
	return c.dsnWithPass(pass)
}

func (c MySQLConfig) dsnWithPass(pass string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", c.Username, pass, c.Host, c.Port, c.Database, c.Options)
}

// PrintDSN ...
func (c MySQLConfig) PrintDSN() string {
	return c.dsnWithPass("[SECRET]")
}

func (c Config) validateRedisServers() {
	serverIDs := map[uint32]struct{}{}
	serverAddrs := map[string]struct{}{}

	if len(c.RedisServers) == 0 {
		panic("redis server list must not be empty")
	}

	for _, s := range c.RedisServers {
		if s.ID <= 0 {
			panic("redis server id must not be empty")
		}
		if len(s.Addr) == 0 {
			panic("redis server address must not be empty")
		}

		_, existed := serverIDs[s.ID]
		if existed {
			panic(fmt.Sprintf("duplicated redis server id '%d'", s.ID))
		}
		serverIDs[s.ID] = struct{}{}

		_, existed = serverAddrs[s.Addr]
		if existed {
			panic(fmt.Sprintf("duplicated redis server address '%s'", s.Addr))
		}
		serverAddrs[s.Addr] = struct{}{}
	}
}
