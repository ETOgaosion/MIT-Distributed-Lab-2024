package shardkv

import "log"

// Debugging
const Debug = false
const DebugVerbose2 = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func D2Printf(format string, a ...interface{}) {
	if DebugVerbose2 {
		log.Printf(format, a...)
	}
}
