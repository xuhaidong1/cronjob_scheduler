package cronjob_scheduler

import (
	"context"
	"fmt"
	"github.com/xuhaidong1/cronjob_scheduler/ioc"
	"testing"
	"time"
)

func TestScheduler_Schedule(t *testing.T) {
	db := ioc.InitDB()
	_ = db.Exec("TRUNCATE TABLE `schedulers`")
	_ = db.Exec("TRUNCATE TABLE `jobs`")
	//scr := NewScheduler(db)
	//scr2 := NewScheduler(db)
	//scr3 := NewScheduler(db)

	//scr := NewScheduler(db, WithLongScheduleStrategy())
	//scr2 := NewScheduler(db, WithLongScheduleStrategy())
	//scr3 := NewScheduler(db, WithLongScheduleStrategy())
	scr := NewScheduler(db, WithLoadBalancePreemptStrategy(100), WithLongScheduleStrategy())
	scr2 := NewScheduler(db, WithLoadBalancePreemptStrategy(100), WithLongScheduleStrategy())
	scr3 := NewScheduler(db, WithLoadBalancePreemptStrategy(100), WithLongScheduleStrategy())
	registerForTest(scr)
	registerForTest(scr2)
	registerForTest(scr3)
	AddJob(scr)
	time.Sleep(time.Second)
	scr.Run()
	scr2.Run()
	scr3.Run()
	ch := make(chan struct{})
	<-ch
}

func AddJob(scr *Scheduler) {
	ctx := context.Background()
	_ = scr.AddJob(ctx, "testjob1", time.Second*5, "*/1 * * * *", 90)
	_ = scr.AddJob(ctx, "testjob2", time.Second*5, "*/1 * * * *", 23)
	_ = scr.AddJob(ctx, "testjob3", time.Second*5, "*/1 * * * *", 92)
	_ = scr.AddJob(ctx, "testjob4", time.Second*5, "*/1 * * * *", 31)
	_ = scr.AddJob(ctx, "testjob5", time.Second*5, "*/1 * * * *", 44)
	_ = scr.AddJob(ctx, "testjob6", time.Second*5, "*/1 * * * *", 65)
	_ = scr.AddJob(ctx, "testjob7", time.Second*5, "*/1 * * * *", 52)
	_ = scr.AddJob(ctx, "testjob8", time.Second*5, "*/1 * * * *", 73)
	_ = scr.AddJob(ctx, "testjob9", time.Second*5, "*/1 * * * *", 84)
	_ = scr.AddJob(ctx, "testjob10", time.Second*5, "*/1 * * * *", 99)
	_ = scr.AddJob(ctx, "testjob11", time.Second*5, "*/1 * * * *", 14)
	_ = scr.AddJob(ctx, "testjob12", time.Second*5, "*/1 * * * *", 22)
	_ = scr.AddJob(ctx, "testjob13", time.Second*5, "*/1 * * * *", 8)
	_ = scr.AddJob(ctx, "testjob14", time.Second*5, "*/1 * * * *", 79)
}

func registerForTest(scr *Scheduler) {
	scr.RegisterJob("testjob1", f)
	scr.RegisterJob("testjob2", f)
	scr.RegisterJob("testjob3", f)
	scr.RegisterJob("testjob4", f)
	scr.RegisterJob("testjob5", f)
	scr.RegisterJob("testjob6", f)
	scr.RegisterJob("testjob7", f)
	scr.RegisterJob("testjob8", f)
	scr.RegisterJob("testjob9", f)
	scr.RegisterJob("testjob10", f)
	scr.RegisterJob("testjob11", f)
	scr.RegisterJob("testjob12", f)
	scr.RegisterJob("testjob13", f)
	scr.RegisterJob("testjob14", f)
}

var f = func(ctx context.Context) error {
	time.Sleep(time.Second)
	return nil
}

func TestCancel(t *testing.T) {
	// 创建一个带有取消功能的 context
	ctx, cancel := context.WithCancel(context.Background())

	// 启动一个goroutine，模拟一个工作任务
	go func() {
		for {
			select {
			case <-ctx.Done():
				// 当收到取消信号时，结束工作
				fmt.Println("Work canceled")
				return
			default:
				// 模拟工作
				fmt.Println("Working...")
				time.Sleep(1 * time.Second)
			}
		}
	}()

	// 模拟一段时间后取消工作
	time.Sleep(3 * time.Second)

	// 第一次调用cancel，发送取消信号
	cancel()

	// 第二次调用cancel，不会产生额外效果
	cancel()

	// 等待一段时间，观察工作是否结束
	time.Sleep(2 * time.Second)
}
