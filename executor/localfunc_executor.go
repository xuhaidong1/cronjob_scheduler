package executor

import (
	"context"
	"fmt"
	"github.com/xuhaidong1/cronjob_scheduler/domain"
)

type LocalFuncExecutor struct {
	funcs map[string]Func
}

func NewLocalFuncExecutor() *LocalFuncExecutor {
	return &LocalFuncExecutor{funcs: make(map[string]Func)}
}

func (l *LocalFuncExecutor) Name() ExecutorType {
	return ExecutorTypeLocalFunc
}

func (l *LocalFuncExecutor) RegisterRunner(name string, r any) {
	f := r.(Func)
	l.funcs[name] = f
}

func (l *LocalFuncExecutor) Exec(ctx context.Context, j domain.Job) error {
	fn, ok := l.funcs[j.Name]
	if !ok {
		return fmt.Errorf("未知任务，你是否注册了？ %s", j.Name)
	}
	return fn(ctx, j)

}

type Func func(ctx context.Context, j domain.Job) error
