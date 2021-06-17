package shardkv

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
	DClient logTopic = "CLNT" // 客户端
	DServer logTopic = "SEVE" // 服务端
	DError  logTopic = "ERRO"
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
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debug >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)

	}
}

func min(a, b int) int { return int(math.Min(float64(a), float64(b))) }

func max(a, b int) int { return int(math.Max(float64(a), float64(b))) }

