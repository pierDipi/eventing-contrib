/*
Copyright 2019 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	"knative.dev/eventing-contrib/pkg/channel"

	"knative.dev/pkg/reconciler"

	"knative.dev/eventing-contrib/natss/pkg/apis/messaging/v1alpha1"
	natssChannelReconciler "knative.dev/eventing-contrib/natss/pkg/client/injection/reconciler/messaging/v1alpha1/natsschannel"
)

const (
	ReconcilerName = "NatssChannel"

	// Name of the corev1.Events emitted from the reconciliation process.
	channelReconciled            = "ChannelReconciled"
	dispatcherDeploymentNotFound = "DispatcherDeploymentDoesNotExist"
	dispatcherServiceNotFound    = "DispatcherServiceDoesNotExist"
	dispatcherEndpointsNotFound  = "DispatcherEndpointsDoesNotExist"
	channelServiceFailed         = "ChannelServiceFailed"

	dispatcherName = "natss-ch-dispatcher"
)

// Reconciler reconciles NATSS Channels.
type Reconciler struct {
	*channel.BaseReconciler
}

var _ natssChannelReconciler.Interface = (*Reconciler)(nil)
var _ natssChannelReconciler.Finalizer = (*Reconciler)(nil)

var _ channel.Reconciler = (*Reconciler)(nil)

func (r *Reconciler) ReconcileKind(ctx context.Context, nc *v1alpha1.NatssChannel) reconciler.Event {
	return r.BaseReconciler.ReconcileKind(ctx, r, nc)
}

func (r *Reconciler) FinalizeKind(_ context.Context, nc *v1alpha1.NatssChannel) reconciler.Event {
	return channel.NewReconciledNormal(nc.Namespace, nc.Name)
}

func (r *Reconciler) Initialize(_ context.Context, _ channel.Channel) (interface{}, error) {
	return nil, nil
}

func (r *Reconciler) Finalize(_ context.Context, _ channel.Channel, _ interface{}) error {
	return nil
}
