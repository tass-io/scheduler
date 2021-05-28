package k8sutils

import (
	"testing"

	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
)

func TestPatch(t *testing.T) {
	result := generatePatchWorkflowRuntime(serverlessv1alpha1.ProcessRuntimes{
		"a": serverlessv1alpha1.ProcessRuntime{Number: 1},
	})
	t.Log(string(result))
}
