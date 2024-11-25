package logger

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type OpenSearchWriter struct {
	endpoint string
	client   *http.Client
}

func NewOpenSearchWriter(endpoint string) *OpenSearchWriter {
	return &OpenSearchWriter{
		endpoint: endpoint,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (w *OpenSearchWriter) Write(p []byte) (n int, err error) {
	req, err := http.NewRequest("POST", w.endpoint, bytes.NewBuffer(p))
	if err != nil {
		return 0, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to send log to OpenSearch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return 0, fmt.Errorf("received unexpected status code from OpenSearch: %d", resp.StatusCode)
	}

	return len(p), nil
}

func openSearchLoggercore() zapcore.Core {
	writer := NewOpenSearchWriter(fmt.Sprintf("%s/%s/_doc", os.Getenv("OPEN_SEARCH_ENDPOINT"), os.Getenv("OPEN_SEARCH_INDEX_NAME")))

	return zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(writer),
		zap.InfoLevel,
	)
}
