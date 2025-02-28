package bpmn

import (
	"hash/adler32"
	"os"

	"github.com/bwmarrin/snowflake"
)

var globalIdGenerator *snowflake.Node = nil

func (state *BpmnEngineState) generateKey() int64 {
	return state.snowflake.Generate().Int64()
}

// getGlobalSnowflakeIdGenerator the global ID generator
// constraints: see also createGlobalSnowflakeIdGenerator
func getGlobalSnowflakeIdGenerator() *snowflake.Node {
	if globalIdGenerator == nil {
		globalIdGenerator = CreateSnowflakeIdGenerator()
	}
	return globalIdGenerator
}

// CreateSnowflakeIdGenerator a new ID generator,
// constraints: creating two new instances within a few microseconds, will create generators with the same seed
func CreateSnowflakeIdGenerator() *snowflake.Node {
	hash32 := adler32.New()
	for _, e := range os.Environ() {
		hash32.Sum([]byte(e))
	}
	snowflakeNode, err := snowflake.NewNode(int64(hash32.Sum32()))
	if err != nil {
		panic("can't initialize snowflake ID generator. Message: " + err.Error())
	}
	return snowflakeNode
}
