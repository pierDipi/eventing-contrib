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
	"errors"

	"github.com/Shopify/sarama"
	"knative.dev/eventing-contrib/pkg/channel"

	"go.uber.org/zap"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"knative.dev/eventing/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"

	"knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
	kafkaScheme "knative.dev/eventing-contrib/kafka/channel/pkg/client/clientset/versioned/scheme"
	kafkaChannelReconciler "knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/reconciler/messaging/v1alpha1/kafkachannel"
	"knative.dev/eventing-contrib/kafka/channel/pkg/reconciler/controller/resources"
	"knative.dev/eventing-contrib/kafka/channel/pkg/utils"
)

const (
	// controllerAgentName is the string used by this controller to identify
	// itself when creating events.
	controllerAgentName = "kafka-ch-controller"

	// Name of the corev1.Events emitted from the reconciliation process.
	dispatcherDeploymentCreated = "DispatcherDeploymentCreated"
	dispatcherDeploymentUpdated = "DispatcherDeploymentUpdated"
	dispatcherServiceCreated    = "DispatcherServiceCreated"
)

func init() {
	// Add run types to the default Kubernetes Scheme so Events can be
	// logged for run types.
	_ = kafkaScheme.AddToScheme(scheme.Scheme)
}

// Reconciler reconciles Kafka Channels.
type Reconciler struct {
	baseReconciler *channel.BaseReconciler

	kafkaConfig      *utils.KafkaConfig
	kafkaConfigError error

	// Using a shared kafkaClusterAdmin does not work currently because of an issue with
	// Shopify/sarama, see https://github.com/Shopify/sarama/issues/1162.
	kafkaClusterAdmin sarama.ClusterAdmin
}

type envConfig struct {
	Image string `envconfig:"DISPATCHER_IMAGE" required:"true"`
}

// Check that our Reconciler implements kafka's injection Interface
var _ kafkaChannelReconciler.Interface = (*Reconciler)(nil)
var _ kafkaChannelReconciler.Finalizer = (*Reconciler)(nil)

// Check that our Reconciler implements common reconciler
var _ channel.Reconciler = (*Reconciler)(nil)

func (r *Reconciler) ReconcileKind(ctx context.Context, kc *v1alpha1.KafkaChannel) pkgreconciler.Event {
	return r.baseReconciler.ReconcileKind(ctx, r, kc)
}

func (r *Reconciler) Initialize(ctx context.Context, c channel.Channel) (interface{}, error) {
	kc := c.(*v1alpha1.KafkaChannel)

	if r.kafkaConfig == nil {
		if r.kafkaConfigError == nil {
			r.kafkaConfigError = errors.New("the config map 'config-kafka' does not exist")
		}
		kc.Status.MarkConfigFailed("MissingConfiguration", "%v", r.kafkaConfigError)
		return nil, r.kafkaConfigError
	}

	kafkaClusterAdmin, err := r.createClient(ctx, kc)
	if err != nil {
		kc.Status.MarkConfigFailed("InvalidConfiguration", "Unable to build Kafka admin client for channel %s: %v", kc.Name, err)
		return nil, err
	}

	kc.Status.MarkConfigTrue()

	if err := r.createTopic(ctx, kc, kafkaClusterAdmin); err != nil {
		kc.Status.MarkTopicFailed("TopicCreateFailed", "error while creating topic: %s", err)
		return nil, err
	}
	kc.Status.MarkTopicTrue()

	return kafkaClusterAdmin, nil
}

func (r *Reconciler) Finalize(_ context.Context, _ channel.Channel, initContext interface{}) error {
	kafkaClusterAdmin := initContext.(sarama.ClusterAdmin)
	return kafkaClusterAdmin.Close()
}

func (r *Reconciler) createClient(_ context.Context, _ *v1alpha1.KafkaChannel) (sarama.ClusterAdmin, error) {
	// We don't currently initialize r.kafkaClusterAdmin, hence we end up creating the cluster admin client every time.
	// This is because of an issue with Shopify/sarama. See https://github.com/Shopify/sarama/issues/1162.
	// Once the issue is fixed we should use a shared cluster admin client. Also, r.kafkaClusterAdmin is currently
	// used to pass a fake admin client in the tests.
	kafkaClusterAdmin := r.kafkaClusterAdmin
	if kafkaClusterAdmin == nil {
		var err error
		kafkaClusterAdmin, err = resources.MakeClient(controllerAgentName, r.kafkaConfig.Brokers)
		if err != nil {
			return nil, err
		}
	}
	return kafkaClusterAdmin, nil
}

func (r *Reconciler) createTopic(ctx context.Context, channel *v1alpha1.KafkaChannel, kafkaClusterAdmin sarama.ClusterAdmin) error {
	logger := logging.FromContext(ctx)

	topicName := utils.TopicName(utils.KafkaChannelSeparator, channel.Namespace, channel.Name)
	logger.Info("Creating topic on Kafka cluster", zap.String("topic", topicName))
	err := kafkaClusterAdmin.CreateTopic(topicName, &sarama.TopicDetail{
		ReplicationFactor: channel.Spec.ReplicationFactor,
		NumPartitions:     channel.Spec.NumPartitions,
	}, false)
	if e, ok := err.(*sarama.TopicError); ok && e.Err == sarama.ErrTopicAlreadyExists {
		return nil
	} else if err != nil {
		logger.Error("Error creating topic", zap.String("topic", topicName), zap.Error(err))
	} else {
		logger.Info("Successfully created topic", zap.String("topic", topicName))
	}
	return err
}

func (r *Reconciler) deleteTopic(ctx context.Context, channel *v1alpha1.KafkaChannel, kafkaClusterAdmin sarama.ClusterAdmin) error {
	logger := logging.FromContext(ctx)

	topicName := utils.TopicName(utils.KafkaChannelSeparator, channel.Namespace, channel.Name)
	logger.Info("Deleting topic on Kafka Cluster", zap.String("topic", topicName))
	err := kafkaClusterAdmin.DeleteTopic(topicName)
	if err == sarama.ErrUnknownTopicOrPartition {
		return nil
	} else if err != nil {
		logger.Error("Error deleting topic", zap.String("topic", topicName), zap.Error(err))
	} else {
		logger.Info("Successfully deleted topic", zap.String("topic", topicName))
	}
	return err
}

func (r *Reconciler) updateKafkaConfig(ctx context.Context, configMap *corev1.ConfigMap) {
	logging.FromContext(ctx).Info("Reloading Kafka configuration")
	kafkaConfig, err := utils.GetKafkaConfig(configMap.Data)
	if err != nil {
		logging.FromContext(ctx).Error("Error reading Kafka configuration", zap.Error(err))
	}
	// For now just override the previous config.
	// Eventually the previous config should be snapshotted to delete Kafka topics
	r.kafkaConfig = kafkaConfig
	r.kafkaConfigError = err
}

func (r *Reconciler) FinalizeKind(ctx context.Context, kc *v1alpha1.KafkaChannel) pkgreconciler.Event {
	// Do not attempt retrying creating the client because it might be a permanent error
	// in which case the finalizer will never get removed.
	if kafkaClusterAdmin, err := r.createClient(ctx, kc); err == nil && r.kafkaConfig != nil {
		if err := r.deleteTopic(ctx, kc, kafkaClusterAdmin); err != nil {
			return err
		}
	}
	return channel.NewReconciledNormal(kc.Namespace, kc.Name) //ok to remove finalizer
}
