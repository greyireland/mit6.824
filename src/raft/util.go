package raft

import (
	"log"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
func heartbeatInterval() time.Duration {
	return time.Millisecond * 100
}
func heartbeatTimeout() time.Duration {
	return time.Duration(rand.Int63()%50+800) * time.Millisecond
}
func electionTimeout() time.Duration {
	return time.Duration(rand.Int63()%150+900) * time.Millisecond
}
