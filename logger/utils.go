package logger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func createOpenSearchIndex(indexName, opensearchURL string) error {
	url := fmt.Sprintf("%s/%s", opensearchURL, indexName)

	indexConfig := map[string]interface{}{
		"settings": map[string]interface{}{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"timestamp": map[string]string{"type": "date"},
				"level":     map[string]string{"type": "keyword"},
				"message":   map[string]string{"type": "text"},
				"caller":    map[string]string{"type": "keyword"},
				"function":  map[string]string{"type": "keyword"},
			},
		},
	}

	payload, err := json.Marshal(indexConfig)
	if err != nil {
		return fmt.Errorf("error marshalling index config: %v", err)
	}

	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("error creating HTTP request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending HTTP request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected response from OpenSearch: %v", resp.Status)
	}

	return nil
}
