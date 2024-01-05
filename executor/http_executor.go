package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/xuhaidong1/cronjob_scheduler/domain"
	"net/http"
)

type HttpConfig struct {
	Endpoint string `json:"endpoint"`
	Method   string `json:"method"`
	Body     string `json:"body"`
}

type HttpExecutor struct {
}

func (h *HttpExecutor) Name() ExecutorType {
	return ExecutorTypeHttp
}

func (h *HttpExecutor) Exec(ctx context.Context, j domain.Job) error {
	var cfg HttpConfig
	err := json.Unmarshal([]byte(j.Config), &cfg)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(cfg.Method, cfg.Endpoint, bytes.NewReader([]byte(cfg.Body)))
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		return errors.New("执行失败")
	}
	return nil
}
