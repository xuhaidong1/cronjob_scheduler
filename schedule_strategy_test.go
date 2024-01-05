package cronjob_scheduler

import (
	"log"
	"testing"
	"time"
)

func TestLongPreemptStrategy_Next(t *testing.T) {
	res := time.Now().Sub(time.Now().Add(time.Minute))
	log.Println(res < 0)
}
