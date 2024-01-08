package executor

import (
	"context"
	"github.com/xuhaidong1/cronjob_scheduler/domain"
)

// Executor 执行器，提供了本地方法执行方式 和 http调用执行方式
type Executor interface {
	// Name Executor的名字
	Name() ExecutorType
	// Exec 真正的执行任务，当ctx.Done时，任务执行会被中断
	Exec(ctx context.Context, j domain.Job) error
	RegisterRunner(name string, r any)
}

type ExecutorType string

const (
	ExecutorTypeLocalFunc ExecutorType = "local"
	ExecutorTypeHttp      ExecutorType = "http"
)
