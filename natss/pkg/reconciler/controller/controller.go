/*
Copyright 2020 The Knative Authors

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

	"github.com/kelseyhightower/envconfig"
	"knative.dev/eventing/pkg/logging"
	"knative.dev/pkg/system"

	"knative.dev/eventing-contrib/pkg/channel"

	"knative.dev/eventing-contrib/natss/pkg/client/injection/informers/messaging/v1alpha1/natsschannel"
	natssChannelReconciler "knative.dev/eventing-contrib/natss/pkg/client/injection/reconciler/messaging/v1alpha1/natsschannel"
	"knative.dev/pkg/controller"
)

const (
	serviceAccountName   = dispatcherName
	configLeaderElection = "config-leader-election-natss"
	messagingRole        = "natss-channel"
)

var (
	dispatcherLabels = map[string]string{
		"messaging.knative.dev/channel": "natss-channel",
		"messaging.knative.dev/role":    "dispatcher",
	}
)

type envConfig struct {
	Image string `envconfig:"DISPATCHER_IMAGE" required:"true"`
}

// NewController initializes the controller and is called by the generated code.
// Registers event handlers to enqueue events.
func NewController(ctx context.Context) *controller.Impl {

	env := &envConfig{}
	if err := envconfig.Process("", env); err != nil {
		logging.FromContext(ctx).Sugar().Panicf("unable to process Natss channel's required environment variables: %v", err)
	}

	if env.Image == "" {
		logging.FromContext(ctx).Panic("unable to process Natss channel's required environment variables (missing DISPATCHER_IMAGE)")
	}

	r := &Reconciler{
		BaseReconciler: channel.NewReconciler(ctx, channel.ReconcilerArgs{
			DispatcherName:       dispatcherName,
			DispatcherImage:      env.Image,
			SystemNamespace:      system.Namespace(),
			ServiceAccountName:   serviceAccountName,
			ConfigLeaderElection: configLeaderElection,
			DispatcherLabels:     dispatcherLabels,
			MessagingRole:        messagingRole,
		}),
	}

	impl := natssChannelReconciler.NewImpl(ctx, r)

	channel.AddHandlers(ctx, r.DispatcherName, impl, natsschannel.Get(ctx).Informer)

	return impl
}
