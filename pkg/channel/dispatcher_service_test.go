package channel

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestMakeDispatcherService(t *testing.T) {

	args := &DispatcherArgs{
		Name:      "dispatcher-name",
		Namespace: "test-ns",
		Labels: map[string]string{
			"l1": "v1",
		},
	}

	want := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      args.Name,
			Namespace: args.Namespace,
			Labels:    args.Labels,
		},
		Spec: corev1.ServiceSpec{
			Selector: args.Labels,
			Ports: []corev1.ServicePort{
				{
					Name:       "http-dispatcher",
					Protocol:   corev1.ProtocolTCP,
					Port:       80,
					TargetPort: intstr.IntOrString{IntVal: 8080},
				},
			},
		},
	}

	got := MakeDispatcherService(args)

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected condition (-want, +got) = %v", diff)
	}
}
