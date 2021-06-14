package fnscheduler

var (
	canCreatePolicies = map[string]func() bool{
		"default": DefaultCanCreateInstancePolicy,
	}
)

// DefaultCanCreateInstancePolicy is a policy to judge whether to create a new process or not
// DefaultCanCreateInstancePolicy always returns true
func DefaultCanCreateInstancePolicy() bool {
	return true
}
