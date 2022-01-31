package server

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/bsm/rumour/internal/rumour"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/httplog"
	"github.com/rs/zerolog"
)

// NewHTTP inits an HTTP server.
func NewHTTP(addr string, state *rumour.State, logOpt httplog.Options) *http.Server {
	return &http.Server{
		Addr:         addr,
		Handler:      newRouter(state, httplog.NewLogger("http", logOpt)),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 300 * time.Second,
		IdleTimeout:  15 * time.Second,
	}
}

func newRouter(state *rumour.State, logger zerolog.Logger) *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Heartbeat("/healthz"))

	r.Route("/v1", func(v1 chi.Router) {
		v1.Use(httplog.RequestLogger(logger))
		v1.Use(middleware.SetHeader("Content-Type", "application/json"))

		v1.Get("/clusters", listClusters(state))
		v1.Get("/clusters/{cluster}", showCluster(state))
		v1.Get("/clusters/{cluster}/topics", listTopics(state))
		v1.Get("/clusters/{cluster}/topics/{topic}", showTopic(state))
		v1.Get("/clusters/{cluster}/consumers", listConsumers(state))
		v1.Get("/clusters/{cluster}/consumers/{consumer}", showConsumer(state))
	})
	return r
}

// --------------------------------------------------------------------

func writeError(w http.ResponseWriter, message string, status int) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error   bool   `json:"error"`
		Message string `json:"message"`
	}{
		Error:   true,
		Message: message,
	})
}

func listClusters(s *rumour.State) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(struct {
			Clusters []string `json:"clusters"`
		}{
			Clusters: s.Clusters(),
		})
	})
}

func showCluster(s *rumour.State) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cluster := chi.URLParam(r, "cluster")
		state := s.Cluster(cluster)
		if state == nil {
			writeError(w, "not found", http.StatusNotFound)
			return
		}

		_ = json.NewEncoder(w).Encode(struct {
			Cluster   string   `json:"cluster"`
			Brokers   []string `json:"brokers"`
			Topics    []string `json:"topics"`
			Consumers []string `json:"consumers"`
		}{
			Cluster:   cluster,
			Brokers:   state.Brokers(),
			Topics:    state.Topics(),
			Consumers: state.ConsumerGroups(),
		})
	})
}

func listTopics(s *rumour.State) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cluster := chi.URLParam(r, "cluster")
		state := s.Cluster(cluster)
		if state == nil {
			writeError(w, "not found", http.StatusNotFound)
			return
		}

		_ = json.NewEncoder(w).Encode(struct {
			Cluster string   `json:"cluster"`
			Topics  []string `json:"topics"`
		}{
			Cluster: cluster,
			Topics:  state.Topics(),
		})
	})
}

func showTopic(s *rumour.State) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cluster := chi.URLParam(r, "cluster")
		state := s.Cluster(cluster)
		if state == nil {
			writeError(w, "not found", http.StatusNotFound)
			return
		}

		topic := chi.URLParam(r, "topic")
		offsets, ok := state.TopicOffsets(topic)
		if !ok {
			writeError(w, "not found", http.StatusNotFound)
			return
		}

		_ = json.NewEncoder(w).Encode(struct {
			Cluster string  `json:"cluster"`
			Topic   string  `json:"topic"`
			Offsets []int64 `json:"offsets"`
		}{
			Cluster: cluster,
			Topic:   topic,
			Offsets: offsets,
		})
	})
}

func listConsumers(s *rumour.State) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cluster := chi.URLParam(r, "cluster")
		state := s.Cluster(cluster)
		if state == nil {
			writeError(w, "not found", http.StatusNotFound)
			return
		}

		_ = json.NewEncoder(w).Encode(struct {
			Cluster   string   `json:"cluster"`
			Consumers []string `json:"consumers"`
		}{
			Cluster:   cluster,
			Consumers: state.ConsumerGroups(),
		})
	})
}

func showConsumer(s *rumour.State) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cluster := chi.URLParam(r, "cluster")
		state := s.Cluster(cluster)
		if state == nil {
			writeError(w, "not found", http.StatusNotFound)
			return
		}

		consumer := chi.URLParam(r, "consumer")
		topics, ok := state.ConsumerTopics(consumer)
		if !ok {
			writeError(w, "not found", http.StatusNotFound)
			return
		}

		_ = json.NewEncoder(w).Encode(struct {
			Cluster  string                 `json:"cluster"`
			Consumer string                 `json:"consumer"`
			Topics   []rumour.ConsumerTopic `json:"topics"`
		}{
			Cluster:  cluster,
			Consumer: consumer,
			Topics:   topics,
		})
	})
}
