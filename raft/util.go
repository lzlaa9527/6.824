package raft

import (
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	DClient  logTopic = "CLNT" // 客户端请求
	DCommit  logTopic = "CMIT" // 提交日志
	DKill    logTopic = "KILL" // server宕机
	DAppend  logTopic = "APET" // AE RPC
	DPersist logTopic = "PERS" // 持久化操作
	DTimer   logTopic = "TIMR" // 定时器操作
	DVote    logTopic = "VOTE" // RV RPC
	DSnap    logTopic = "SNAP" // 快照
	DTerm    logTopic = "TERM" // 修改任期
	DTest    logTopic = "TEST" // 测试信息
	DServer  logTopic = "SEVE"
	DError   logTopic = "ERRO"
	DWarn    logTopic = "WARN"
)

var debugStart time.Time
var debug = 1

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("DEBUG") // DEBUG = 1打印输出，否则不打印
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func init() {
	debugStart = time.Now()
	// debug = getVerbosity()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

	path := "./output/raft_" + strconv.Itoa(time.Now().Second()) + ".log"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("fail to open `%s`, err:%s\n", path, err)
	}
	log.SetOutput(f)
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debug >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("  ###  %06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func Min(a, b int) int { return int(math.Min(float64(a), float64(b))) }

func Max(a, b int) int { return int(math.Max(float64(a), float64(b))) }
