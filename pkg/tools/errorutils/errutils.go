package errorutils

type ResourceLimitError struct {
	error
}

func (rle *ResourceLimitError) Error() string {
	return "resource is not allowed to create process"
}
