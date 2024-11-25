package webhook

import (
	"bytes"
	"encoding/json"
	"net/http"
)

type DiscordMessage struct {
	Title       string                `json:"title"`
	Description string                `json:"description"`
	Color       int                   `json:"color"`
	Fields      []DiscordMessageField `json:"fields"`
	Webhook     string                `json:"webhook"`
}

type DiscordMessageField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	InLine bool   `json:"inline"`
}

func NewDiscordMessage() DiscordMessage {
	return DiscordMessage{
		Color: 0x3498db,
	}
}

func (m DiscordMessage) WithTitle(title string) DiscordMessage {
	m.Title = title
	return m
}

func (m DiscordMessage) WithDescription(descritpion string) DiscordMessage {
	m.Description = descritpion
	return m
}

func (m DiscordMessage) WithFields(field DiscordMessageField) DiscordMessage {
	m.Fields = append(m.Fields, field)
	return m
}

func (m DiscordMessage) WithWebhook(webhook string) DiscordMessage {
	m.Webhook = webhook
	return m
}

func (m DiscordMessage) SendDiscordEmbedWithFields() error {

	embed := map[string]interface{}{
		"title":       m.Title,
		"description": m.Description,
		"color":       0x3498db,
		"fields":      m.Fields,
	}

	payload := map[string]interface{}{
		"embeds": []map[string]interface{}{embed},
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", m.Webhook, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return err
	}

	return nil
}
