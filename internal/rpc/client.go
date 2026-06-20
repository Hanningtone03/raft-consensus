package rpc

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"
)

func Call(host string, port int, endpoint string, payload interface{}, result interface{}) error {
	url := "http://" + host + ":" + portToString(port) + "/" + endpoint

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return json.NewDecoder(resp.Body).Decode(result)
}

func portToString(port int) string {
	digits := []byte{}
	if port == 0 {
		return "0"
	}
	for port > 0 {
		digits = append([]byte{byte('0' + port%10)}, digits...)
		port /= 10
	}
	return string(digits)
}
