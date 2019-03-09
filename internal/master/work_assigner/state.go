package work_assigner

type state string

const (
	assigningNewWork              = state("assigningNewWork")
	waitingForLastWorksToComplete = state("waitingForLastWorksToComplete")
	allWorkIsComplete             = state("allWorkIsComplete")
)
