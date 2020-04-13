package channel

import (
	"context"
	"fmt"
	"reflect"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	rbacv1listers "k8s.io/client-go/listers/rbac/v1"
	"knative.dev/eventing/pkg/apis/eventing"
	"knative.dev/eventing/pkg/logging"
	"knative.dev/eventing/pkg/reconciler/names"
	"knative.dev/pkg/apis"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment"
	"knative.dev/pkg/client/injection/kube/informers/core/v1/endpoints"
	"knative.dev/pkg/client/injection/kube/informers/core/v1/service"
	"knative.dev/pkg/client/injection/kube/informers/core/v1/serviceaccount"
	"knative.dev/pkg/client/injection/kube/informers/rbac/v1/rolebinding"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/reconciler"
)

const (
	dispatcherDeploymentCreated     = "DispatcherDeploymentCreated"
	dispatcherDeploymentUpdated     = "DispatcherDeploymentUpdated"
	dispatcherDeploymentFailed      = "DispatcherDeploymentFailed"
	dispatcherServiceCreated        = "DispatcherServiceCreated"
	dispatcherServiceFailed         = "DispatcherServiceFailed"
	dispatcherServiceAccountCreated = "DispatcherServiceAccountCreated"
	dispatcherRoleBindingCreated    = "DispatcherRoleBindingCreated"
)

// Channel is the interface that wraps all required interfaces and methods.
type Channel interface {
	apis.Defaultable
	apis.Validatable
	runtime.Object
	metav1.Object
	kmeta.OwnerRefable

	// GetStatus returns the status of the channel (see Status)
	GetStatus() Status
}

// Status is the interface that has all methods to update the status of the channel
type Status interface {
	// InitializeConditions sets relevant unset conditions to Unknown state.
	InitializeConditions()

	// MarkDispatcherFailed sets the dispatcher status to False.
	MarkDispatcherFailed(reason, messageFormat string, messageA ...interface{})
	// MarkDispatcherUnknown sets the dispatcher status to Unknown.
	MarkDispatcherUnknown(reason, messageFormat string, messageA ...interface{})
	// PropagateDispatcherStatus propagates the dispatcher status based on ds.
	PropagateDispatcherStatus(ds *appsv1.DeploymentStatus)

	// MarkServiceFailed sets the dispatcher's service status to False.
	MarkServiceFailed(reason, messageFormat string, messageA ...interface{})
	// MarkServiceUnknown sets the dispatcher's service status to Unknown.
	MarkServiceUnknown(reason, messageFormat string, messageA ...interface{})
	// MarkServiceTrue sets the dispatcher's service status to True.
	MarkServiceTrue()

	// MarkEndpointsFailed sets the endpoint status to False.
	MarkEndpointsFailed(reason, messageFormat string, messageA ...interface{})
	// MarkEndpointsTrue sets the endpoint status to True.
	MarkEndpointsTrue()

	// MarkChannelServiceFailed sets the channel's service status to False.
	MarkChannelServiceFailed(reason, messageFormat string, messageA ...interface{})
	// MarkChannelServiceTrue sets the channel's service status to True.
	MarkChannelServiceTrue()

	// SetAddress sets the address (as part of Addressable contract) and marks the correct condition.
	SetAddress(url *apis.URL)
}

// Reconciler is the interface that allows to add behaviour to the BaseReconciler.
type Reconciler interface {
	// Initialize is the first method called in the reconciliation loop.
	// It could return an object that will be passed as parameter to the Finalize method.
	Initialize(ctx context.Context, c Channel) (interface{}, error)

	// Finalize is the last method called in the reconciliation loop.
	// The initContext parameter is the same object returned by the Initialize method.
	Finalize(ctx context.Context, c Channel, initContext interface{}) error
}

type DispatcherArgs struct {
	Name                     string
	Scope                    string
	Namespace                string
	Image                    string
	Labels                   map[string]string
	ServiceAccountName       string
	ConfigLeaderElectionName string
}

type ChannelServiceArgs struct {
	MessagingRoleLabel string
	MessagingRole      string
	PortName           string
	Port               int32
}

// ServiceOption can be used to optionally modify the K8s service in MakeK8sService.
type ServiceOption func(*corev1.Service) error

type DispatcherOption func(deployment *appsv1.Deployment) *appsv1.Deployment

type BaseReconciler struct {
	KubeClientSet kubernetes.Interface

	ServiceAccountLister corev1listers.ServiceAccountLister
	RoleBindingLister    rbacv1listers.RoleBindingLister
	DeploymentLister     appsv1listers.DeploymentLister
	ServiceLister        corev1listers.ServiceLister
	EndpointsLister      corev1listers.EndpointsLister

	DispatcherName           string
	DispatcherImage          string
	DispatcherLabels         map[string]string
	SystemNamespace          string
	ServiceAccountName       string
	ConfigLeaderElectionName string
	DispatcherOptions        []DispatcherOption
	MessagingRole            string
}

type ReconcilerArgs struct {
	DispatcherOptions    []DispatcherOption
	DispatcherName       string
	DispatcherImage      string
	SystemNamespace      string
	ServiceAccountName   string
	ConfigLeaderElection string
	DispatcherLabels     map[string]string
	MessagingRole        string
}

// NewReconciler creates a new BaseReconciler.
func NewReconciler(ctx context.Context, args ReconcilerArgs) *BaseReconciler {
	return &BaseReconciler{
		KubeClientSet:            kubeclient.Get(ctx),
		ServiceAccountLister:     serviceaccount.Get(ctx).Lister(),
		RoleBindingLister:        rolebinding.Get(ctx).Lister(),
		DeploymentLister:         deployment.Get(ctx).Lister(),
		ServiceLister:            service.Get(ctx).Lister(),
		EndpointsLister:          endpoints.Get(ctx).Lister(),
		DispatcherName:           args.DispatcherName,
		DispatcherImage:          args.DispatcherImage,
		DispatcherLabels:         args.DispatcherLabels,
		SystemNamespace:          args.SystemNamespace,
		ServiceAccountName:       args.ServiceAccountName,
		ConfigLeaderElectionName: args.ConfigLeaderElection,
		DispatcherOptions:        args.DispatcherOptions,
		MessagingRole:            args.MessagingRole,
	}
}

func (r *BaseReconciler) ReconcileKind(ctx context.Context, reconciler Reconciler, c Channel) reconciler.Event {

	c.GetStatus().InitializeConditions()

	logger := logging.FromContext(ctx)

	c.SetDefaults(ctx)
	if err := c.Validate(ctx); err != nil {
		logger.Error("invalid channel", zap.String("channel", c.GetName()), zap.Error(err))
		return err
	}

	// We reconcile the status of the Channel by looking at:
	// 1. Initialization
	// 2. Dispatcher Deployment for it's readiness.
	// 3. Dispatcher k8s Service for it's existence.
	// 4. Dispatcher endpoints to ensure that there's something backing the Service.
	// 5. K8s service representing the channel that will use ExternalName to point to the Dispatcher k8s service.
	// 6. Finalization

	initContext, err := reconciler.Initialize(ctx, c)
	if err != nil {
		return err
	}
	defer func() {
		if err := reconciler.Finalize(ctx, c, initContext); err != nil {
			logger.Error("cannot finalize channel", zap.Error(err))
		}
	}()

	scope, ok := c.GetAnnotations()[eventing.ScopeAnnotationKey]
	if !ok {
		scope = eventing.ScopeCluster
	}

	dispatcherNamespace := r.SystemNamespace
	if scope == eventing.ScopeNamespace {
		dispatcherNamespace = c.GetNamespace()
	}

	args := DispatcherArgs{
		Name:                     r.DispatcherName,
		Scope:                    scope,
		Namespace:                dispatcherNamespace,
		Image:                    r.DispatcherImage,
		Labels:                   r.DispatcherLabels,
		ServiceAccountName:       r.ServiceAccountName,
		ConfigLeaderElectionName: r.ConfigLeaderElectionName,
	}

	channelServiceArgs := ChannelServiceArgs{
		MessagingRoleLabel: "messaging.knative.dev/role",
		MessagingRole:      r.MessagingRole,
		PortName:           "http",
		Port:               80,
	}

	// Make sure the dispatcher deployment exists and propagate the status to the Channel
	if _, err := r.reconcileDispatcher(ctx, args, c); err != nil {
		return err
	}

	// Make sure the dispatcher service exists and propagate the status to the Channel in case it does not exist.
	// We don't do anything with the service because it's status contains nothing useful, so just do
	// an existence check. Then below we check the endpoints targeting it.
	if _, err := r.reconcileDispatcherService(ctx, args, c); err != nil {
		return err
	}

	// Get the Dispatcher Service Endpoints and propagate the status to the Channel
	// endpoints has the same name as the service, so not a bug.
	if _, err := r.reconcileEndpoints(ctx, dispatcherNamespace, c); err != nil {
		return err
	}

	svc, err := r.reconcileChannelService(ctx, &channelServiceArgs, args, c)
	if err != nil {
		return err
	}

	c.GetStatus().MarkChannelServiceTrue()
	c.GetStatus().SetAddress(&apis.URL{
		Scheme: "http",
		Host:   names.ServiceHostName(svc.Name, svc.Namespace),
	})

	// Ok, so now the Dispatcher Deployment & Service have been created, we're golden since the
	// dispatcher watches the Channel and where it needs to dispatch events to.
	return NewReconciledNormal(c.GetNamespace(), c.GetName())
}

func (r *BaseReconciler) reconcileDispatcher(ctx context.Context, args DispatcherArgs, c Channel) (*appsv1.Deployment, error) {
	if args.Scope == eventing.ScopeNamespace {

		sa, err := r.reconcileServiceAccount(ctx, args.Namespace, c)
		if err != nil {
			return nil, err
		}

		_, err = r.reconcileRoleBinding(ctx, r.DispatcherName, args.Namespace, c, r.DispatcherName, sa)
		if err != nil {
			return nil, err
		}

		// Reconcile the RoleBinding allowing read access to the shared configmaps.
		// Note this RoleBinding is created in the system namespace and points to a
		// subject in the dispatcher's namespace.
		// TODO: might change when ConfigMapPropagation lands
		roleBindingName := fmt.Sprintf("%s-%s", r.DispatcherName, args.Namespace)
		_, err = r.reconcileRoleBinding(ctx, roleBindingName, r.SystemNamespace, c, "eventing-config-reader", sa)
		if err != nil {
			return nil, err
		}
	}

	expected := MakeDispatcherDeployment(&args, r.DispatcherOptions...)
	d, err := r.DeploymentLister.Deployments(args.Namespace).Get(r.DispatcherName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			d, err := r.KubeClientSet.AppsV1().Deployments(args.Namespace).Create(expected)
			if err == nil {
				controller.GetEventRecorder(ctx).Event(c, corev1.EventTypeNormal, dispatcherDeploymentCreated, "Dispatcher deployment created")
				c.GetStatus().PropagateDispatcherStatus(&d.Status)
				return d, err
			} else {
				c.GetStatus().MarkDispatcherFailed(dispatcherDeploymentFailed, "Failed to create the dispatcher deployment: %v", err)
				return d, NewDeploymentWarn(err)
			}
		}

		logging.FromContext(ctx).Error("Unable to get the dispatcher deployment", zap.Error(err))
		c.GetStatus().MarkDispatcherUnknown("DispatcherDeploymentFailed", "Failed to get dispatcher deployment: %v", err)
		return nil, err
	} else if !reflect.DeepEqual(expected.Spec.Template.Spec.Containers[0].Image, d.Spec.Template.Spec.Containers[0].Image) {
		logging.FromContext(ctx).Sugar().Infof("Deployment image is not what we expect it to be, updating Deployment Got: %q Expect: %q", expected.Spec.Template.Spec.Containers[0].Image, d.Spec.Template.Spec.Containers[0].Image)
		d, err := r.KubeClientSet.AppsV1().Deployments(args.Namespace).Update(expected)
		if err == nil {
			controller.GetEventRecorder(ctx).Event(c, corev1.EventTypeNormal, dispatcherDeploymentUpdated, "Dispatcher deployment updated")
			c.GetStatus().PropagateDispatcherStatus(&d.Status)
			return d, nil
		} else {
			c.GetStatus().MarkServiceFailed("DispatcherDeploymentUpdateFailed", "Failed to update the dispatcher deployment: %v", err)
		}
		return d, NewDeploymentWarn(err)
	}

	c.GetStatus().PropagateDispatcherStatus(&d.Status)
	return d, nil
}

func (r *BaseReconciler) reconcileServiceAccount(ctx context.Context, dispatcherNamespace string, c Channel) (*corev1.ServiceAccount, error) {
	sa, err := r.ServiceAccountLister.ServiceAccounts(dispatcherNamespace).Get(r.DispatcherName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			expected := MakeServiceAccount(dispatcherNamespace, r.DispatcherName)
			sa, err := r.KubeClientSet.CoreV1().ServiceAccounts(dispatcherNamespace).Create(expected)
			if err == nil {
				controller.GetEventRecorder(ctx).Event(c, corev1.EventTypeNormal, dispatcherServiceAccountCreated, "Dispatcher service account created")
				return sa, nil
			} else {
				c.GetStatus().MarkDispatcherFailed("DispatcherDeploymentFailed", "Failed to create the dispatcher service account: %v", err)
				return sa, NewServiceAccountWarn(err)
			}
		}

		c.GetStatus().MarkDispatcherUnknown("DispatcherServiceAccountFailed", "Failed to get dispatcher service account: %v", err)
		return nil, NewServiceAccountWarn(err)
	}
	return sa, err
}

func (r *BaseReconciler) reconcileRoleBinding(ctx context.Context, name string, namespace string, c Channel, clusterRoleName string, sa *corev1.ServiceAccount) (*rbacv1.RoleBinding, error) {
	rb, err := r.RoleBindingLister.RoleBindings(namespace).Get(name)
	if err != nil {
		if apierrs.IsNotFound(err) {
			expected := MakeRoleBinding(namespace, name, sa, clusterRoleName)
			rb, err := r.KubeClientSet.RbacV1().RoleBindings(namespace).Create(expected)
			if err == nil {
				controller.GetEventRecorder(ctx).Event(c, corev1.EventTypeNormal, dispatcherRoleBindingCreated, "Dispatcher role binding created")
				return rb, nil
			} else {
				c.GetStatus().MarkDispatcherFailed("DispatcherDeploymentFailed", "Failed to create the dispatcher role binding: %v", err)
				return rb, NewRoleBindingWarn(err)
			}
		}
		c.GetStatus().MarkDispatcherUnknown("DispatcherRoleBindingFailed", "Failed to get dispatcher role binding: %v", err)
		return nil, NewRoleBindingWarn(err)
	}
	return rb, err
}

func (r *BaseReconciler) reconcileDispatcherService(ctx context.Context, args DispatcherArgs, c Channel) (*corev1.Service, error) {
	svc, err := r.ServiceLister.Services(args.Namespace).Get(r.DispatcherName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			expected := MakeDispatcherService(&args)
			svc, err := r.KubeClientSet.CoreV1().Services(args.Namespace).Create(expected)

			if err == nil {
				controller.GetEventRecorder(ctx).Event(c, corev1.EventTypeNormal, dispatcherServiceCreated, "Dispatcher service created")
				c.GetStatus().MarkServiceTrue()
			} else {
				logging.FromContext(ctx).Error("Unable to create the dispatcher service", zap.Error(err))
				controller.GetEventRecorder(ctx).Eventf(c, corev1.EventTypeWarning, dispatcherServiceFailed, "Failed to create the dispatcher service: %v", err)
				c.GetStatus().MarkServiceFailed("DispatcherServiceFailed", "Failed to create the dispatcher service: %v", err)
				return svc, err
			}

			return svc, err
		}

		c.GetStatus().MarkServiceUnknown("DispatcherServiceFailed", "Failed to get dispatcher service: %v", err)
		return nil, NewDispatcherServiceWarn(err)
	}

	c.GetStatus().MarkServiceTrue()
	return svc, nil
}

func (r *BaseReconciler) reconcileEndpoints(ctx context.Context, dispatcherNamespace string, c Channel) (*corev1.Endpoints, error) {
	e, err := r.EndpointsLister.Endpoints(dispatcherNamespace).Get(r.DispatcherName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			c.GetStatus().MarkEndpointsFailed("DispatcherEndpointsDoesNotExist", "Dispatcher Endpoints does not exist")
		} else {
			logging.FromContext(ctx).Error("Unable to get the dispatcher endpoints", zap.Error(err))
			c.GetStatus().MarkEndpointsFailed("DispatcherEndpointsGetFailed", "Failed to get dispatcher endpoints")
		}
		return nil, err
	}

	if len(e.Subsets) == 0 {
		logging.FromContext(ctx).Error("No endpoints found for Dispatcher service", zap.Error(err))
		c.GetStatus().MarkEndpointsFailed("DispatcherEndpointsNotReady", "There are no endpoints ready for Dispatcher service")
		return nil, fmt.Errorf("there are no endpoints ready for Dispatcher service %s", r.DispatcherName)
	}

	c.GetStatus().MarkEndpointsTrue()
	return e, nil
}

func (r *BaseReconciler) reconcileChannelService(ctx context.Context, channelArgs *ChannelServiceArgs, args DispatcherArgs, c Channel) (*corev1.Service, error) {
	// Get the  Service and propagate the status to the Channel in case it does not exist.
	// We don't do anything with the service because it's status contains nothing useful, so just do
	// an existence check. Then below we check the endpoints targeting it.
	// We may change this name later, so we have to ensure we use proper addressable when resolving these.
	expected, err := MakeK8sService(c, channelArgs, ExternalService(args.Namespace, args.Name))
	if err != nil {
		logging.FromContext(ctx).Error("failed to create the channel service object", zap.Error(err))
		c.GetStatus().MarkChannelServiceFailed("ChannelServiceFailed", fmt.Sprintf("Channel Service failed: %s", err))
		return nil, err
	}

	svc, err := r.ServiceLister.Services(c.GetNamespace()).Get(MakeChannelServiceName(c.GetName()))
	if err != nil {
		if apierrs.IsNotFound(err) {
			svc, err = r.KubeClientSet.CoreV1().Services(c.GetNamespace()).Create(expected)
			if err != nil {
				logging.FromContext(ctx).Error("failed to create the channel service object", zap.Error(err))
				c.GetStatus().MarkChannelServiceFailed("ChannelServiceFailed", fmt.Sprintf("Channel Service failed: %s", err))
				return nil, err
			}
			return svc, nil
		}
		logging.FromContext(ctx).Error("Unable to get the channel service", zap.Error(err))
		return nil, err
	} else if !equality.Semantic.DeepEqual(svc.Spec, expected.Spec) {
		svc = svc.DeepCopy()
		svc.Spec = expected.Spec

		svc, err = r.KubeClientSet.CoreV1().Services(c.GetNamespace()).Update(svc)
		if err != nil {
			logging.FromContext(ctx).Error("Failed to update the channel service", zap.Error(err))
			return nil, err
		}
	}
	// Check to make sure that the Channel owns this service and if not, complain.
	if !metav1.IsControlledBy(svc, c) {
		err := fmt.Errorf("channel: %s/%s does not own Service: %q", c.GetNamespace(), c.GetName(), svc.Name)
		c.GetStatus().MarkChannelServiceFailed("ChannelServiceFailed", fmt.Sprintf("Channel Service failed: %s", err))
		return nil, err
	}

	return svc, nil
}

func NewServiceAccountWarn(err error) reconciler.Event {
	return reconciler.NewEvent(corev1.EventTypeWarning, "DispatcherServiceAccountFailed", "Reconciling dispatcher ServiceAccount failed: %s", err)
}

func NewRoleBindingWarn(err error) reconciler.Event {
	return reconciler.NewEvent(corev1.EventTypeWarning, "DispatcherRoleBindingFailed", "Reconciling dispatcher RoleBinding failed: %s", err)
}

func NewDeploymentWarn(err error) reconciler.Event {
	return reconciler.NewEvent(corev1.EventTypeWarning, "DispatcherDeploymentFailed", "Reconciling dispatcher Deployment failed with: %s", err)
}

func NewDispatcherServiceWarn(err error) reconciler.Event {
	return reconciler.NewEvent(corev1.EventTypeWarning, "DispatcherServiceFailed", "Reconciling dispatcher Service failed with: %s", err)
}

func NewReconciledNormal(namespace, name string) reconciler.Event {
	return reconciler.NewEvent(corev1.EventTypeNormal, "ChannelReconciled", "Channel reconciled: \"%s/%s\"", namespace, name)
}
