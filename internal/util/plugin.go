package util

import (
	"log"
	"plugin"

	"github.com/ruggeri/nedreduce/internal/types"
)

var pluginPath = "./build/plugin.so"

// SetPluginPath is used to determine where the plugin with the user
// mapper/reducer functions is loaded from. The default is good, but
// tests need to load from elsewhere.
func SetPluginPath(newPluginPath string) {
	pluginPath = newPluginPath
}

// LoadMappingFunctionByName loads the plugin, gets the specified
// mapping function, and casts it to the expected type.
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

// LoadReducingFunctionByName loads the plugin, gets the specified
// reducing function, and casts it to the expected type.
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
