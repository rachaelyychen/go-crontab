package test

import (
	"fmt"
	"gocrontab/utils"
	"testing"
	"time"
)

func TestSplit(t *testing.T) {
	str := "a..b...c"
	res := utils.Split(str, ".")
	fmt.Printf("\n%v\n", res)
}

func TestTime(t *testing.T) {
	// 1567504876 (s)
	fmt.Println(time.Now().Unix())
	// 1567504876117634617 (ns)
	fmt.Println(time.Now().UnixNano())
	// 2019-09-03 03:01:16
	fmt.Println(time.Now().Format("2006-01-02 15:04:05"))

	start := time.Now()
	time.Sleep(10 * time.Second)
	fmt.Println(time.Since(start))
}
