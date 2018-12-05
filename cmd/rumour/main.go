package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bsm/rumour/internal/rumour"
	"github.com/bsm/rumour/internal/server"
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
		HTTPAddr string   `default:":8080"`
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
	srv := server.NewHTTP(rc.HTTPAddr, state)

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
