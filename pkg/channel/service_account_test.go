package channel

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNewServiceAccount(t *testing.T) {

	testNs := "test-ns"
	serviceAccountName := "sa-test"

	want := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNs,
			Name:      serviceAccountName,
		},
	}

	got := MakeServiceAccount(testNs, serviceAccountName)

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected condition (-want, +got) = %v", diff)
	}
}
