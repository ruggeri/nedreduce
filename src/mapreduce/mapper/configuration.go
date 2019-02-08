package mapper

// A Configuration describes the settings for this map task.
type Configuration struct {
	JobName             string
	MapTaskIdx          int
	MapperInputFileName string
	NumReducers         int
	MappingFunction     MappingFunction
}
