package internal

import (
	"math"
	"sort"
)

type IsOutlier interface {
	IsOutlier(load int64, loads []int64) bool
}

type DefaultIsOutlier struct {
}

func (d DefaultIsOutlier) IsOutlier(load int64, loads []int64) bool {
	data := float64(load)
	datas := make([]float64, len(loads))
	for i := 0; i < len(loads); i++ {
		datas[i] = float64(loads[i])
	}
	// 设置阈值
	const threshold float64 = 1.60
	return ZScore(data, datas) > threshold
}

func ZScore(dataPoint float64, datas []float64) float64 {
	// 计算均值和标准偏差
	mean, stdDev := calculateMeanAndStdDev(datas)
	return (dataPoint - mean) / stdDev
}

func isOutlier(zScore float64, threshold float64) bool {
	return zScore > threshold
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
