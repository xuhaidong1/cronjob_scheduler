package cronjob_scheduler

import (
	"context"
	"github.com/xuhaidong1/cronjob_scheduler/ioc"
	"log"
	"testing"
	"time"
)

func TestScheduler_Schedule(t *testing.T) {
	scr := NewScheduler(ioc.InitDB(), LongPreemptStrategyType, TimeoutPreemptStrategyType)
	_ = scr.RegisterJob(context.Background(), "testjob1", time.Second*5, "*/1 * * * *", func(ctx context.Context) error {
		log.Println("testjob1在跑")
		time.Sleep(time.Second)
		log.Println("testjob1在跑2")
		return nil
	})
	_ = scr.RegisterJob(context.Background(), "testjob2", time.Second*5, "*/1 * * * *", func(ctx context.Context) error {
		log.Println("testjob2在跑!!")
		time.Sleep(time.Second)
		log.Println("testjob2在跑ing!")
		return nil
	})
	_ = scr.RegisterJob(context.Background(), "testjob3", time.Second*5, "*/1 * * * *", func(ctx context.Context) error {
		log.Println("testjob3在跑!!")
		time.Sleep(time.Second)
		log.Println("testjob3在跑ing!")
		return nil
	})
	scr2 := NewScheduler(ioc.InitDB(), LongPreemptStrategyType, TimeoutPreemptStrategyType)
	_ = scr2.RegisterJob(context.Background(), "testjob1", time.Second*5, "*/1 * * * *", func(ctx context.Context) error {
		log.Println("testjob1在跑")
		time.Sleep(time.Second)
		log.Println("testjob1在跑2")
		return nil
	})
	_ = scr2.RegisterJob(context.Background(), "testjob2", time.Second*5, "*/1 * * * *", func(ctx context.Context) error {
		log.Println("testjob2在跑!!")
		time.Sleep(time.Second)
		log.Println("testjob2在跑ing!")
		return nil
	})
	_ = scr2.RegisterJob(context.Background(), "testjob3", time.Second*5, "*/1 * * * *", func(ctx context.Context) error {
		log.Println("testjob3在跑!!")
		time.Sleep(time.Second)
		log.Println("testjob3在跑ing!")
		return nil
	})
	ch := make(chan struct{})
	<-ch
}
