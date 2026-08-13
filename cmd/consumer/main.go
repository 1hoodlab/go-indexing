package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/config"
	"github.com/1hoodlab/go-indexing.git/internal/storage/postgres"
)

func main() {
	cfg := config.Load()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store, err := postgres.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer store.Close()

	if cfg.WebhookURL != "" {
		if _, err := store.UpsertWebhookEndpoint(ctx, "default", cfg.WebhookURL, cfg.WebhookSecret); err != nil {
			log.Printf("webhook endpoint upsert: %v", err)
		}
	}

	client := &http.Client{Timeout: 30 * time.Second}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	log.Printf("consumer started (id=%s)", cfg.ConsumerID)
	for {
		select {
		case <-ctx.Done():
			log.Println("consumer shutting down")
			return
		case <-ticker.C:
			if err := pollOnce(ctx, store, client, cfg); err != nil {
				log.Printf("poll error: %v", err)
			}
		}
	}
}

func pollOnce(ctx context.Context, store *postgres.Store, client *http.Client, cfg config.Config) error {
	endpoints, err := store.LoadEnabledWebhookEndpoints(ctx)
	if err != nil {
		return err
	}
	if len(endpoints) == 0 && cfg.WebhookURL == "" {
		return nil
	}
	if len(endpoints) == 0 {
		endpoints = []postgres.WebhookEndpoint{{ID: 0, Name: "env", URL: cfg.WebhookURL, Secret: cfg.WebhookSecret, Enabled: true}}
	}

	rows, err := store.PollOutbox(ctx, cfg.ConsumerID, 50)
	if err != nil {
		return err
	}
	for _, row := range rows {
		body := row.Payload
		if len(body) == 0 || string(body) == "{}" {
			payload, err := store.BuildOutboxPayload(ctx, row.LogID)
			if err != nil {
				log.Printf("build payload log %d: %v", row.LogID, err)
				continue
			}
			body, _ = json.Marshal(payload)
		}
		for _, ep := range endpoints {
			start := time.Now()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.URL, bytes.NewReader(body))
			if err != nil {
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			if row.Topic != "" {
				req.Header.Set("X-Outbox-Topic", row.Topic)
			}
			if ep.Secret != "" {
				mac := hmac.New(sha256.New, []byte(ep.Secret))
				mac.Write(body)
				req.Header.Set("X-Signature", hex.EncodeToString(mac.Sum(nil)))
			}
			resp, err := client.Do(req)
			duration := int(time.Since(start).Milliseconds())
			if err != nil {
				_ = store.MarkOutboxFailed(ctx, row.OutboxID, ep.ID, err.Error(), 0, time.Now().Add(time.Minute))
				continue
			}
			respBody, _ := json.Marshal(map[string]any{"status": resp.StatusCode})
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				_ = store.MarkOutboxDelivered(ctx, row.OutboxID, ep.ID, resp.StatusCode, string(respBody), duration)
			} else {
				_ = store.MarkOutboxFailed(ctx, row.OutboxID, ep.ID, "http error", resp.StatusCode, time.Now().Add(time.Minute))
			}
		}
	}
	return nil
}
