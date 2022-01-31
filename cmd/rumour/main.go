package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bsm/rumour/internal/rumour"
	"github.com/bsm/rumour/internal/server"
	"github.com/go-chi/httplog"
	"github.com/kelseyhightower/envconfig"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatalln(err)
	}
}

func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var rc struct {
		Clusters []string `default:"default"`
		HTTP     struct {
			Addr string `default:":8080"`
		}
		Log struct {
			Level string `default:"info"`
			JSON  bool   `default:"false"`
			Tags  map[string]string
		}
	}
	if err := envconfig.Process("rumour", &rc); err != nil {
		return err
	}

	var cc []rumour.ClusterConfig
	for _, cluster := range rc.Clusters {
		c := rumour.ClusterConfig{Name: cluster}
		if err := envconfig.Process("rumour_"+cluster, &c); err != nil {
			return err
		}
		cc = append(cc, c)
	}

	state := rumour.NewState(rc.Clusters)
	fetcher, err := rumour.NewFetcher(cc...)
	if err != nil {
		return err
	}
	srv := server.NewHTTP(rc.HTTP.Addr, state, httplog.Options{
		LogLevel: rc.Log.Level,
		JSON:     rc.Log.JSON,
		Tags:     rc.Log.Tags,
	})

	go fetcher.RunLoop(ctx, state)
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
		<-sigs

		cancel()
		srv.Shutdown(context.Background())
	}()

	return srv.ListenAndServe()
}
