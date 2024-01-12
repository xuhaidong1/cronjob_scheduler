package cronjob_scheduler

import (
	"context"
	"fmt"
	"github.com/xuhaidong1/cronjob_scheduler/ioc"
	"math"
	"math/rand"
	"sort"
	"strconv"
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
	var scrs = make([]*Scheduler, 5)
	for i := 0; i < len(scrs); i++ {
		scrs[i] = NewScheduler(db, WithLongScheduleStrategy())
		//scrs[i] = NewScheduler(db, WithLoadBalancePreemptStrategy(), WithLongScheduleStrategy(), WithLimitLoadDownGradeStrategy(150, HighWeightFirst))
	}
	for i := 0; i < len(scrs); i++ {
		registerForTest(scrs[i])
	}
	AddJob(scrs[0])
	time.Sleep(time.Second)
	ctx := context.Background()
	for i := 0; i < len(scrs); i++ {
		scrs[i].Run(ctx)
	}
	go func() {
		time.Sleep(time.Second * 63)
		scrs[1].SetDownGrade(true)
		scrs[2].SetDownGrade(true)
		scrs[3].SetDownGrade(true)
		time.Sleep(time.Second * 60)
		scrs[1].SetDownGrade(false)
		scrs[2].SetDownGrade(false)
		scrs[3].SetDownGrade(false)
	}()
	ch := make(chan struct{})
	<-ch
}

func AddJob(scr *Scheduler) {
	ctx := context.Background()
	for i := 1; i < 30; i++ {
		_ = scr.AddJob(ctx, "testjob"+strconv.Itoa(i), time.Second*5, "*/1 * * * *", int64(rand.Intn(100)))
	}
}

func registerForTest(scr *Scheduler) {
	for i := 1; i < 30; i++ {
		scr.RegisterJobFunc("testjob"+strconv.Itoa(i), f)
	}
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

func TestZscore(t *testing.T) {
	data := []float64{539,
		136,
		104,
		119,
		635,
	}

	// 计算均值和标准偏差
	mean, stdDev := calculateMeanAndStdDev(data)

	// 设置阈值
	threshold := 1.5

	// 检测离群点
	for _, dataPoint := range data {
		zScore := calculateZScore(dataPoint, mean, stdDev)
		if isOutlier(zScore, threshold) {
			fmt.Printf("Outlier detected: %v (Z-Score: %v)\n", dataPoint, zScore)
		}
	}
}

func calculateZScore(dataPoint float64, mean float64, stdDev float64) float64 {
	return (dataPoint - mean) / stdDev
}

func isOutlier(zScore float64, threshold float64) bool {
	return math.Abs(zScore) > threshold
}

func calculateMeanAndStdDev(data []float64) (float64, float64) {
	// 计算中位数
	sort.Float64s(data)
	mean := data[len(data)/2]

	// 计算标准偏差
	variance := 0.0
	for _, value := range data {
		variance += math.Pow(value-mean, 2)
	}
	variance /= float64(len(data))

	stdDev := math.Sqrt(variance)

	return mean, stdDev
}
