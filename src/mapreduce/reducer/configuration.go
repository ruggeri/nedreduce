package reducer

// A Configuration describes the settings for this reduce task.
type Configuration struct {
	JobName          string
	NumMappers       int
	ReduceTaskIdx    int
	ReducingFunction ReducingFunction
}
