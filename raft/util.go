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
	dClient  logTopic = "CLNT" // [%d] S%d Append Entry, IN:%d, TE:%d - %v
	dCommit  logTopic = "CMIT" // [%d] S%d commit Entry, LA:%d, CI:%d - %v
	dKill    logTopic = "KILL" // [%d] S%d was Killed.
	dAppend  logTopic = "APET" // [%d] S%d Send AE RPC to S%d, PLI:%d, PLT:%d
	dPersist logTopic = "PERS" // [%d] S%d Saved State, T:%d, VF:%d
	dTimer   logTopic = "TIMR" // [%d] S%d convert to , RE:%s
	// [%d] S%d Append entries from S%d, PLI:%d, PLT:%d - CLI:%d, CLT:%d
	// [%d] S%d Refuse AE RPC from S%d, TE:%d, CT:%d

	dVote logTopic = "VOTE" // [%d] S%d Send RV RPC to S%d
	// [%d] S%d Get vote from S%d, TVs:%d
	// [%d] S%d Grant vote to S%d
	// [%d] S%d Refuse RV RPC to S%d, RE:%s

	dLog   logTopic = "LOG1"
	dLog2  logTopic = "LOG2"
	dSnap  logTopic = "SNAP"
	dTerm  logTopic = "TERM"
	dTest  logTopic = "TEST"
	dTrace logTopic = "TRCE"
	dError logTopic = "ERRO"

	dWarn logTopic = "WARN"
)

var debugStart time.Time
var debug = 1

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")	// VERBOSE = 1打印Debug输出，否则不打印
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
	debug = getVerbosity()
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
