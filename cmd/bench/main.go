package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/infra"
)

func main() {
	var (
		mode          = flag.String("mode", "all", "execution mode: clean | seed | run | all")
		mysqlHost     = flag.String("mysql-host", "127.0.0.1", "MySQL host")
		mysqlPort     = flag.Int("mysql-port", 3306, "MySQL port")
		mysqlUser     = flag.String("mysql-user", "root", "MySQL user")
		mysqlPass     = flag.String("mysql-pass", "wallflow_dev_mysql", "MySQL password")
		mysqlDB       = flag.String("mysql-db", "wallpaper_feed_bench", "Dedicated benchmark MySQL database")
		mysqlMaxConns = flag.Int("mysql-max-conns", 50, "MySQL max_open_conns")
		redisHost     = flag.String("redis-host", "127.0.0.1", "Redis host")
		redisPort     = flag.Int("redis-port", 6379, "Redis port")
		redisPass     = flag.String("redis-pass", "", "Redis password")
		redisDB       = flag.Int("redis-db", 1, "Dedicated benchmark Redis DB index")
		mqHost        = flag.String("mq-host", "127.0.0.1", "RabbitMQ host")
		mqPort        = flag.Int("mq-port", 5672, "RabbitMQ port")
		mqUser        = flag.String("mq-user", "wallflow", "RabbitMQ user")
		mqPass        = flag.String("mq-pass", "wallflow_dev_mq", "RabbitMQ password")
		mqVhost       = flag.String("mq-vhost", "/bench", "Dedicated benchmark RabbitMQ vhost")
		migrationsDir = flag.String("migrations", "migrations", "Path to SQL migrations directory")
		wallpapers    = flag.Int("wallpapers", 30000, "Number of wallpapers to seed")
		suites        = flag.String("suites", "read,write,feed", "Comma-separated benchmark suites to run: read,write,feed")
	)
	flag.Parse()

	ctx := context.Background()

	if err := ensureBenchDatabase(ctx, *mysqlHost, *mysqlPort, *mysqlUser, *mysqlPass, *mysqlDB, *migrationsDir); err != nil {
		fmt.Fprintf(os.Stderr, "ensure bench database failed: %v\n", err)
		os.Exit(1)
	}

	cfg := &config.Config{
		Server: config.ServerConfig{Port: 0, Env: "prod"},
		MySQL: config.MySQLConfig{
			Host:         *mysqlHost,
			Port:         *mysqlPort,
			User:         *mysqlUser,
			Password:     *mysqlPass,
			DBName:       *mysqlDB,
			MaxOpenConns: *mysqlMaxConns,
		},
		Redis: config.RedisConfig{
			Host:     *redisHost,
			Port:     *redisPort,
			Password: *redisPass,
			DB:       *redisDB,
		},
		RabbitMQ: config.RabbitMQConfig{
			Enabled:  true,
			Host:     *mqHost,
			Port:     *mqPort,
			User:     *mqUser,
			Password: *mqPass,
			Vhost:    *mqVhost,
		},
		Storage: config.StorageConfig{
			Provider: config.StorageProviderNoop,
		},
		JWTSecret: benchJWTSecret,
	}

	switch *mode {
	case "clean":
		db, err := infra.OpenMySQL(ctx, cfg.MySQL, false)
		if err != nil {
			fatalf("open mysql: %v", err)
		}
		defer func() { _ = infra.CloseMySQL(db) }()
		rdb, err := infra.OpenRedis(ctx, cfg.Redis)
		if err != nil {
			fatalf("open redis: %v", err)
		}
		defer func() { _ = rdb.Close() }()
		if err := cleanBenchData(ctx, db, rdb); err != nil {
			fatalf("clean bench data: %v", err)
		}
		fmt.Println("[clean] Done.")

	case "seed":
		db, err := infra.OpenMySQL(ctx, cfg.MySQL, false)
		if err != nil {
			fatalf("open mysql: %v", err)
		}
		defer func() { _ = infra.CloseMySQL(db) }()
		rdb, err := infra.OpenRedis(ctx, cfg.Redis)
		if err != nil {
			fatalf("open redis: %v", err)
		}
		defer func() { _ = rdb.Close() }()
		seedCfg := defaultSeedConfig()
		seedCfg.Wallpapers = *wallpapers
		if _, err := seedBenchData(ctx, db, rdb, seedCfg); err != nil {
			fatalf("seed bench data: %v", err)
		}

	case "run", "all":
		var meta *SeedMeta
		if *mode == "all" {
			db, err := infra.OpenMySQL(ctx, cfg.MySQL, false)
			if err != nil {
				fatalf("open mysql: %v", err)
			}
			rdb, err := infra.OpenRedis(ctx, cfg.Redis)
			if err != nil {
				_ = infra.CloseMySQL(db)
				fatalf("open redis: %v", err)
			}
			seedCfg := defaultSeedConfig()
			seedCfg.Wallpapers = *wallpapers
			meta, err = seedBenchData(ctx, db, rdb, seedCfg)
			_ = rdb.Close()
			_ = infra.CloseMySQL(db)
			if err != nil {
				fatalf("seed bench data: %v", err)
			}
		} else {
			db, err := infra.OpenMySQL(ctx, cfg.MySQL, false)
			if err != nil {
				fatalf("open mysql: %v", err)
			}
			meta, err = loadSeedMeta(ctx, db)
			_ = infra.CloseMySQL(db)
			if err != nil {
				fatalf("load seed meta: %v", err)
			}
		}

		suite, err := newBenchSuite(ctx, cfg, meta)
		if err != nil {
			fatalf("init bench suite: %v", err)
		}
		defer suite.close()

		selected := make(map[string]bool)
		for part := range strings.SplitSeq(*suites, ",") {
			selected[strings.TrimSpace(strings.ToLower(part))] = true
		}

		var allResults []BenchResult
		if selected["read"] {
			res, err := suite.runGroupARead(ctx)
			if err != nil {
				fatalf("group A read failed: %v", err)
			}
			allResults = append(allResults, res...)
		}
		if selected["write"] {
			res, err := suite.runGroupBWrite(ctx)
			if err != nil {
				fatalf("group B write failed: %v", err)
			}
			allResults = append(allResults, res...)
		}
		if selected["feed"] {
			res, err := suite.runGroupCFeed(ctx)
			if err != nil {
				fatalf("group C feed failed: %v", err)
			}
			allResults = append(allResults, res...)
		}

		printResultTable(allResults)

	default:
		fatalf("unknown mode %q (expected clean | seed | run | all)", *mode)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
