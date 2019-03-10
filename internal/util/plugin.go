package util

import (
	"log"
	"plugin"

	"github.com/ruggeri/nedreduce/internal/types"
)

var pluginPath string = "./build/plugin.so"

func SetPluginPath(newPluginPath string) {
	pluginPath = newPluginPath
}

func LoadMappingFunctionByName(mappingFunctionName string) types.MappingFunction {
	p, err := plugin.Open(pluginPath)
	if err != nil {
		log.Panicf("Error loading plugin: %v\n", err)
	}

	fn, err := p.Lookup(mappingFunctionName)
	if err != nil {
		log.Panicf("Error loading plugin: %v\n", err)
	}

	return types.MappingFunction(fn.(func(string, string, types.EmitterFunction)))
}

func LoadReducingFunctionByName(reducingFunctionName string) types.ReducingFunction {
	p, err := plugin.Open(pluginPath)
	if err != nil {
		log.Panicf("Error loading plugin: %v\n", err)
	}

	fn, err := p.Lookup(reducingFunctionName)
	if err != nil {
		log.Panicf("Error loading plugin: %v\n", err)
	}

	return types.ReducingFunction(
		fn.(func(
			string,
			types.GroupIteratorFunction,
			types.EmitterFunction,
		)),
	)
}
