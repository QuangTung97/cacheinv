package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/QuangTung97/eventx"
	"github.com/QuangTung97/go-memcache/memcache"
	"github.com/dustin/go-humanize"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/QuangTung97/cacheinv"
	"github.com/QuangTung97/cacheinv/config"
	memcache_client "github.com/QuangTung97/cacheinv/memcache"
	"github.com/QuangTung97/cacheinv/mysql"
	redis_client "github.com/QuangTung97/cacheinv/redis"

	_ "github.com/go-sql-driver/mysql" // import mysql driver
)

func printSep() {
	fmt.Println("----------------------------------")
}

func initRepo(conf config.Config) cacheinv.Repository {
	printSep()
	fmt.Println("Connect to MySQL:", conf.MySQL.PrintDSN())
	fmt.Println("MySQL MaxOpenConns:", conf.MySQL.MaxOpenConns)
	fmt.Println("MySQL MaxIdleConns:", conf.MySQL.MaxIdleConns)
	fmt.Println("MySQL Max Conn Idle Time:", conf.MySQL.MaxConnIdleTime)

	db := sqlx.MustOpen("mysql", conf.MySQL.DSN())
	db.SetMaxOpenConns(int(conf.MySQL.MaxOpenConns))
	db.SetMaxIdleConns(int(conf.MySQL.MaxIdleConns))
	db.SetConnMaxIdleTime(conf.MySQL.MaxConnIdleTime)

	fmt.Println("event_table_name:", conf.EventTableName)
	fmt.Println("offset_table_name:", conf.OffsetTableName)

	return mysql.NewRepository(db, conf.EventTableName, conf.OffsetTableName)
}

func initRedisClient(conf config.Config) cacheinv.Client {
	clients := map[int64]*redis.Client{}

	for _, redisConf := range conf.RedisServers {
		fmt.Printf("Connect to Redis: '%s'\n", redisConf.Addr)
		redisClient := redis.NewClient(&redis.Options{
			Addr: redisConf.Addr,
		})
		clients[int64(redisConf.ID)] = redisClient
	}

	return redis_client.NewClient(clients)
}

func initMemcacheClient(conf config.Config) cacheinv.Client {
	clients := map[int64]*memcache.Client{}

	for _, mcConf := range conf.MemcacheServers {
		fmt.Printf("Connect to Memcache: '%s'\n", mcConf.Addr)
		redisClient, err := memcache.New(mcConf.Addr, 1)
		if err != nil {
			panic(err)
		}
		clients[int64(mcConf.ID)] = redisClient
	}

	return memcache_client.NewClient(clients)
}

func initClient(conf config.Config) cacheinv.Client {
	printSep()

	if conf.ClientType == config.ClientTypeRedis {
		return initRedisClient(conf)
	}
	return initMemcacheClient(conf)
}

// Start ...
func Start() {
	conf := config.Load()

	repo := initRepo(conf)
	client := initClient(conf)

	printSep()
	if conf.EventRetentionSize <= 10 {
		panic("event_retention_size is too small")
	}

	fmt.Println("Event Retention Size:", humanize.FormatInteger("#,###.", int(conf.EventRetentionSize)))
	fmt.Println("DB Scan Duration:", conf.DBScanDuration)

	job := cacheinv.NewInvalidatorJob(
		repo, client,
		cacheinv.WithRunnerOptions(
			eventx.WithDBProcessorRetryTimer(conf.DBScanDuration),
		),
		cacheinv.WithRetentionOptions(
			eventx.WithMaxTotalEvents(uint64(conf.EventRetentionSize)),
			eventx.WithDeleteBatchSize(32),
		),
	)

	mux := &http.ServeMux{}

	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/health/live", healthCheck)
	mux.HandleFunc("/health/ready", healthCheck)

	mux.HandleFunc("/notify", func(writer http.ResponseWriter, request *http.Request) {
		if len(conf.NotifyAccessToken) > 0 {
			val := request.Header.Get("X-Notify-Access-Token")
			if val != conf.NotifyAccessToken {
				writer.WriteHeader(http.StatusForbidden)
				_, _ = writer.Write([]byte("Invalid access token"))
				return
			}
		}

		job.Notify()
		_, _ = writer.Write([]byte("Success"))
	})

	startJobAndServer(conf, mux, job)
}

func healthCheck(w http.ResponseWriter, _ *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"code":0,"message":"Success"}`))
}

func startJobAndServer(
	conf config.Config, mux *http.ServeMux,
	job *cacheinv.InvalidatorJob,
) {
	printSep()
	fmt.Printf("Listen HTTP on Port: %d\n", conf.HTTPPort)
	fmt.Println("Access Token Len:", len(conf.NotifyAccessToken))

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", conf.HTTPPort),
		Handler: mux,
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGKILL)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		job.Run()
	}()

	go func() {
		defer wg.Done()
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		if err != nil {
			panic(err)
		}
	}()

	<-sigChan

	job.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := httpServer.Shutdown(ctx)
	if err != nil {
		panic(err)
	}

	wg.Wait()

	fmt.Println("Graceful Shutdown Completed")
}
