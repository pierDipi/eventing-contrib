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
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/informers/messaging/v1alpha1/kafkachannel"
	kafkaChannelReconciler "knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/reconciler/messaging/v1alpha1/kafkachannel"
	"knative.dev/eventing-contrib/kafka/channel/pkg/reconciler/controller/resources"
	"knative.dev/eventing-contrib/pkg/channel"
	"knative.dev/eventing/pkg/logging"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/system"
)

const (
	dispatcherName       = "kafka-ch-dispatcher"
	serviceAccountName   = dispatcherName
	configLeaderElection = "config-leader-election-kafka"
	messagingRole        = "kafka-channel"
)

var (
	dispatcherLabels = map[string]string{
		"messaging.knative.dev/channel": "kafka-channel",
		"messaging.knative.dev/role":    "dispatcher",
	}
)

// NewController initializes the controller and is called by the generated code.
// Registers event handlers to enqueue events.
func NewController(
	ctx context.Context,
	cmw configmap.Watcher,
) *controller.Impl {

	env := &envConfig{}
	if err := envconfig.Process("", env); err != nil {
		logging.FromContext(ctx).Sugar().Panicf("unable to process Kafka channel's required environment variables: %v", err)
	}

	if env.Image == "" {
		logging.FromContext(ctx).Panic("unable to process Kafka channel's required environment variables (missing DISPATCHER_IMAGE)")
	}

	r := &Reconciler{
		baseReconciler: channel.NewReconciler(ctx, channel.ReconcilerArgs{
			DispatcherName:       dispatcherName,
			DispatcherImage:      env.Image,
			SystemNamespace:      system.Namespace(),
			ServiceAccountName:   serviceAccountName,
			ConfigLeaderElection: configLeaderElection,
			DispatcherLabels:     dispatcherLabels,
			DispatcherOptions:    []channel.DispatcherOption{resources.WithConfigVolume()},
			MessagingRole:        messagingRole,
		}),
	}

	impl := kafkaChannelReconciler.NewImpl(ctx, r)

	// Get and Watch the Kafka config map and dynamically update Kafka configuration.
	if _, err := kubeclient.Get(ctx).CoreV1().ConfigMaps(system.Namespace()).Get("config-kafka", metav1.GetOptions{}); err == nil {
		cmw.Watch("config-kafka", func(configMap *v1.ConfigMap) {
			r.updateKafkaConfig(ctx, configMap)
		})
	} else if !apierrors.IsNotFound(err) {
		logging.FromContext(ctx).With(zap.Error(err)).Fatal("Error reading ConfigMap 'config-kafka'")
	}

	channel.AddHandlers(ctx, dispatcherName, impl, kafkachannel.Get(ctx).Informer)

	return impl
}
