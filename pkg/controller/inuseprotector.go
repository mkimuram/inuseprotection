/*
Copyright 2021 The Kubernetes Authors.

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
	goerrors "errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	"github.com/mkimuram/inuseprotection/pkg/util/useeref"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/controller-manager/pkg/informerfactory"

	// import known versions
	_ "k8s.io/client-go/kubernetes"
)

// ResourceResyncTime defines the resync period of the informers.
const ResourceResyncTime time.Duration = 0

// InUseProtector runs reflectors to watch for changes of managed API
// objects, funnels the results to a single-threaded dependencyGraphBuilder,
// which builds a graph caching the dependencies among objects. Triggered by the
// graph changes, the dependencyGraphBuilder enqueues objects that can
// potentially be deleted to the `attemptToDelete` queue, and enqueues
// objects whose dependents need to be orphaned to the `attemptToOrphan` queue.
// The InUseProtector has workers who consume these two queues, send requests
// to the API server to delete/update the objects accordingly.
// Note that having the dependencyGraphBuilder notify the garbage collector
// ensures that the garbage collector operates with a graph that is at least as
// up to date as the notification is sent.
type InUseProtector struct {
	restMapper     ResettableRESTMapper
	metadataClient metadata.Interface
	// in-use protector attempts to delete the finalizer from the items in attemptToDelete queue when no dependent exists.
	attemptToDelete workqueue.RateLimitingInterface
	// in-use protector adds finalizer to the items in addToProtection queue.
	addToProtection workqueue.RateLimitingInterface
	// in-use protector removes finalizer from the items in removeFromProtection queue.
	removeFromProtection   workqueue.RateLimitingInterface
	dependencyGraphBuilder *GraphBuilder
	// GC caches the usees that do not exist according to the API server.
	absentUseeCache *ReferenceCache

	workerLock sync.RWMutex
}

// NewInUseProtector creates a new InUseProtector.
func NewInUseProtector(
	kubeClient clientset.Interface,
	metadataClient metadata.Interface,
	mapper ResettableRESTMapper,
	ignoredResources map[schema.GroupResource]struct{},
	sharedInformers informerfactory.InformerFactory,
	informersStarted <-chan struct{},
) (*InUseProtector, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	eventRecorder := eventBroadcaster.NewRecorder(runtime.NewScheme(), v1.EventSource{Component: "in-use-protector-controller"})

	attemptToDelete := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "in_use_protector_controller_attempt_to_delete")
	addToProtection := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "in_use_protector_controller_add_to_protection")
	removeFromProtection := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "in_use_protector_controller_remove_from_protection")
	absentUseeCache := NewReferenceCache(500)
	gc := &InUseProtector{
		metadataClient:       metadataClient,
		restMapper:           mapper,
		attemptToDelete:      attemptToDelete,
		addToProtection:      addToProtection,
		removeFromProtection: removeFromProtection,
		absentUseeCache:      absentUseeCache,
	}
	gc.dependencyGraphBuilder = &GraphBuilder{
		eventRecorder:    eventRecorder,
		metadataClient:   metadataClient,
		informersStarted: informersStarted,
		restMapper:       mapper,
		graphChanges:     workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "in_use_protector_controller_graph_changes"),
		uidToNode: &concurrentUIDToNode{
			uidToNode: make(map[types.UID]*node),
		},
		attemptToDelete:      attemptToDelete,
		addToProtection:      addToProtection,
		removeFromProtection: removeFromProtection,
		absentUseeCache:      absentUseeCache,
		sharedInformers:      sharedInformers,
		ignoredResources:     ignoredResources,
	}

	return gc, nil
}

// resyncMonitors starts or stops resource monitors as needed to ensure that all
// (and only) those resources present in the map are monitored.
func (gc *InUseProtector) resyncMonitors(deletableResources map[schema.GroupVersionResource]struct{}) error {
	if err := gc.dependencyGraphBuilder.syncMonitors(deletableResources); err != nil {
		return err
	}
	gc.dependencyGraphBuilder.startMonitors()
	return nil
}

// Run starts in-use protector workers.
func (gc *InUseProtector) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer gc.attemptToDelete.ShutDown()
	//defer gc.attemptToOrphan.ShutDown()
	defer gc.dependencyGraphBuilder.graphChanges.ShutDown()

	klog.Infof("Starting in-use protector controller")
	defer klog.Infof("Shutting down in-use protector controller")

	go gc.dependencyGraphBuilder.Run(stopCh)

	if !cache.WaitForNamedCacheSync("in-use protector", stopCh, gc.dependencyGraphBuilder.IsSynced) {
		return
	}

	klog.Infof("In-use protector: all resource monitors have synced. Proceeding to in-use protect")

	// workers
	for i := 0; i < workers; i++ {
		go wait.Until(gc.runAttemptToDeleteWorker, 1*time.Second, stopCh)
		go wait.Until(gc.runAddToProtectionWorker, 1*time.Second, stopCh)
		go wait.Until(gc.runRemoveFromProtectionWorker, 1*time.Second, stopCh)
	}

	<-stopCh
}

// ResettableRESTMapper is a RESTMapper which is capable of resetting itself
// from discovery.
type ResettableRESTMapper interface {
	meta.RESTMapper
	Reset()
}

// Sync periodically resyncs the in-use protector when new resources are
// observed from discovery. When new resources are detected, Sync will stop all
// workers, reset gc.restMapper, and resync the monitors.
//
// Note that discoveryClient should NOT be shared with gc.restMapper, otherwise
// the mapper's underlying discovery client will be unnecessarily reset during
// the course of detecting new resources.
func (gc *InUseProtector) Sync(discoveryClient discovery.ServerResourcesInterface, period time.Duration, stopCh <-chan struct{}) {
	oldResources := make(map[schema.GroupVersionResource]struct{})
	wait.Until(func() {
		// Get the current resource list from discovery.
		newResources := GetDeletableResources(discoveryClient)

		// This can occur if there is an internal error in GetDeletableResources.
		if len(newResources) == 0 {
			klog.V(2).Infof("no resources reported by discovery, skipping in-use protector sync")
			return
		}

		// Decide whether discovery has reported a change.
		if reflect.DeepEqual(oldResources, newResources) {
			klog.V(5).Infof("no resource updates from discovery, skipping in-use protector sync")
			return
		}

		// Ensure workers are paused to avoid processing events before informers
		// have resynced.
		gc.workerLock.Lock()
		defer gc.workerLock.Unlock()

		// Once we get here, we should not unpause workers until we've successfully synced
		attempt := 0
		wait.PollImmediateUntil(100*time.Millisecond, func() (bool, error) {
			attempt++

			// On a reattempt, check if available resources have changed
			if attempt > 1 {
				newResources = GetDeletableResources(discoveryClient)
				if len(newResources) == 0 {
					klog.V(2).Infof("no resources reported by discovery (attempt %d)", attempt)
					return false, nil
				}
			}

			klog.V(2).Infof("syncing in-use protector with updated resources from discovery (attempt %d): %s", attempt, printDiff(oldResources, newResources))

			// Resetting the REST mapper will also invalidate the underlying discovery
			// client. This is a leaky abstraction and assumes behavior about the REST
			// mapper, but we'll deal with it for now.
			gc.restMapper.Reset()
			klog.V(4).Infof("reset restmapper")

			// Perform the monitor resync and wait for controllers to report cache sync.
			//
			// NOTE: It's possible that newResources will diverge from the resources
			// discovered by restMapper during the call to Reset, since they are
			// distinct discovery clients invalidated at different times. For example,
			// newResources may contain resources not returned in the restMapper's
			// discovery call if the resources appeared in-between the calls. In that
			// case, the restMapper will fail to map some of newResources until the next
			// attempt.
			if err := gc.resyncMonitors(newResources); err != nil {
				utilruntime.HandleError(fmt.Errorf("failed to sync resource monitors (attempt %d): %v", attempt, err))
				return false, nil
			}
			klog.V(4).Infof("resynced monitors")

			// wait for caches to fill for a while (our sync period) before attempting to rediscover resources and retry syncing.
			// this protects us from deadlocks where available resources changed and one of our informer caches will never fill.
			// informers keep attempting to sync in the background, so retrying doesn't interrupt them.
			// the call to resyncMonitors on the reattempt will no-op for resources that still exist.
			// note that workers stay paused until we successfully resync.
			if !cache.WaitForNamedCacheSync("in-use protector", waitForStopOrTimeout(stopCh, period), gc.dependencyGraphBuilder.IsSynced) {
				utilruntime.HandleError(fmt.Errorf("timed out waiting for dependency graph builder sync during sync (attempt %d)", attempt))
				return false, nil
			}

			// success, break out of the loop
			return true, nil
		}, stopCh)

		// Finally, keep track of our new state. Do this after all preceding steps
		// have succeeded to ensure we'll retry on subsequent syncs if an error
		// occurred.
		oldResources = newResources
		klog.V(2).Infof("synced in-use protector")
	}, period, stopCh)
}

// printDiff returns a human-readable summary of what resources were added and removed
func printDiff(oldResources, newResources map[schema.GroupVersionResource]struct{}) string {
	removed := sets.NewString()
	for oldResource := range oldResources {
		if _, ok := newResources[oldResource]; !ok {
			removed.Insert(fmt.Sprintf("%+v", oldResource))
		}
	}
	added := sets.NewString()
	for newResource := range newResources {
		if _, ok := oldResources[newResource]; !ok {
			added.Insert(fmt.Sprintf("%+v", newResource))
		}
	}
	return fmt.Sprintf("added: %v, removed: %v", added.List(), removed.List())
}

// waitForStopOrTimeout returns a stop channel that closes when the provided stop channel closes or when the specified timeout is reached
func waitForStopOrTimeout(stopCh <-chan struct{}, timeout time.Duration) <-chan struct{} {
	stopChWithTimeout := make(chan struct{})
	go func() {
		select {
		case <-stopCh:
		case <-time.After(timeout):
		}
		close(stopChWithTimeout)
	}()
	return stopChWithTimeout
}

// IsSynced returns true if dependencyGraphBuilder is synced.
func (gc *InUseProtector) IsSynced() bool {
	return gc.dependencyGraphBuilder.IsSynced()
}

func (gc *InUseProtector) runAttemptToDeleteWorker() {
	for gc.attemptToDeleteWorker() {
	}
}

var enqueuedVirtualDeleteEventErr = goerrors.New("enqueued virtual delete event")

var namespacedUseeOfClusterScopedObjectErr = goerrors.New("cluster-scoped objects cannot refer to namespaced usees")

func (gc *InUseProtector) attemptToDeleteWorker() bool {
	item, quit := gc.attemptToDelete.Get()
	gc.workerLock.RLock()
	defer gc.workerLock.RUnlock()
	if quit {
		return false
	}
	defer gc.attemptToDelete.Done(item)
	n, ok := item.(*node)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("expect *node, got %#v", item))
		return true
	}

	if !n.isObserved() {
		nodeFromGraph, existsInGraph := gc.dependencyGraphBuilder.uidToNode.Read(n.identity.UID)
		if !existsInGraph {
			// this can happen if attemptToDelete loops on a requeued virtual node because attemptToDeleteItem returned an error,
			// and in the meantime a deletion of the real object associated with that uid was observed
			klog.V(5).Infof("item %s no longer in the graph, skipping attemptToDeleteItem", n)
			return true
		}
		if nodeFromGraph.isObserved() {
			// this can happen if attemptToDelete loops on a requeued virtual node because attemptToDeleteItem returned an error,
			// and in the meantime the real object associated with that uid was observed
			klog.V(5).Infof("item %s no longer virtual in the graph, skipping attemptToDeleteItem on virtual node", n)
			return true
		}
	}

	err := gc.attemptToDeleteItem(n)
	if err == enqueuedVirtualDeleteEventErr {
		// a virtual event was produced and will be handled by processGraphChanges, no need to requeue this node
		return true
	} else if err == namespacedUseeOfClusterScopedObjectErr {
		// a cluster-scoped object referring to a namespaced usee is an error that will not resolve on retry, no need to requeue this node
		return true
	} else if err != nil {
		if _, ok := err.(*restMappingError); ok {
			// There are at least two ways this can happen:
			// 1. The reference is to an object of a custom type that has not yet been
			//    recognized by gc.restMapper (this is a transient error).
			// 2. The reference is to an invalid group/version. We don't currently
			//    have a way to distinguish this from a valid type we will recognize
			//    after the next discovery sync.
			// For now, record the error and retry.
			klog.V(5).Infof("error syncing item %s: %v", n, err)
		} else {
			utilruntime.HandleError(fmt.Errorf("error syncing item %s: %v", n, err))
		}
		// retry if garbage collection of an object failed.
		gc.attemptToDelete.AddRateLimited(item)
	} else if !n.isObserved() {
		// requeue if item hasn't been observed via an informer event yet.
		// otherwise a virtual node for an item added AND removed during watch reestablishment can get stuck in the graph and never removed.
		// see https://issue.k8s.io/56121
		klog.V(5).Infof("item %s hasn't been observed via informer yet", n.identity)
		gc.attemptToDelete.AddRateLimited(item)
	}
	return true
}

func useeRefsToUIDs(refs []useeref.UseeReference) []types.UID {
	var ret []types.UID
	for _, ref := range refs {
		ret = append(ret, ref.UID)
	}
	return ret
}

// attemptToDeleteItem looks up the live API object associated with the node,
// and issues a delete IFF the uid matches and has no dependent.
//
// if the API get request returns a NotFound error, or the retrieved item's uid does not match,
// a virtual delete event for the node is enqueued and enqueuedVirtualDeleteEventErr is returned.
func (gc *InUseProtector) attemptToDeleteItem(item *node) error {
	klog.V(2).InfoS("Processing object", "object", klog.KRef(item.identity.Namespace, item.identity.Name),
		"objectUID", item.identity.UID, "kind", item.identity.Kind, "virtual", !item.isObserved())

	// "being deleted" is an one-way trip to the final deletion. We'll just wait for the final deletion, and then process the object's dependents.
	if item.isBeingDeleted() {
		klog.V(5).Infof("processing item %s returned at once, because its DeletionTimestamp is non-nil", item.identity)
		return nil
	}
	// TODO: It's only necessary to talk to the API server if this is a
	// "virtual" node. The local graph could lag behind the real status, but in
	// practice, the difference is small.
	latest, err := gc.getObject(item.identity)
	switch {
	case errors.IsNotFound(err):
		// the GraphBuilder can add "virtual" node for a usee that doesn't
		// exist yet, so we need to enqueue a virtual Delete event to remove
		// the virtual node from GraphBuilder.uidToNode.
		klog.V(5).Infof("item %v not found, generating a virtual delete event", item.identity)
		gc.dependencyGraphBuilder.enqueueVirtualDeleteEvent(item.identity)
		return enqueuedVirtualDeleteEventErr
	case err != nil:
		return err
	}

	if latest.GetUID() != item.identity.UID {
		klog.V(5).Infof("UID doesn't match, item %v not found, generating a virtual delete event", item.identity)
		gc.dependencyGraphBuilder.enqueueVirtualDeleteEvent(item.identity)
		return enqueuedVirtualDeleteEventErr
	}

	if item.dependentsLength() != 0 {
		// item doesn't have any user, so it needs to be deleted.
		return gc.removeFinalizer(item, FinalizerDeleteDependents)
	}

	return nil
}

// *FOR TEST USE ONLY*
// GraphHasUID returns if the GraphBuilder has a particular UID store in its
// uidToNode graph. It's useful for debugging.
// This method is used by integration tests.
func (gc *InUseProtector) GraphHasUID(u types.UID) bool {
	_, ok := gc.dependencyGraphBuilder.uidToNode.Read(u)
	return ok
}

// GetDeletableResources returns all resources from discoveryClient that the
// garbage collector should recognize and work with. More specifically, all
// preferred resources which support the 'delete', 'list', and 'watch' verbs.
//
// All discovery errors are considered temporary. Upon encountering any error,
// GetDeletableResources will log and return any discovered resources it was
// able to process (which may be none).
func GetDeletableResources(discoveryClient discovery.ServerResourcesInterface) map[schema.GroupVersionResource]struct{} {
	preferredResources, err := discoveryClient.ServerPreferredResources()
	if err != nil {
		if discovery.IsGroupDiscoveryFailedError(err) {
			klog.Warningf("failed to discover some groups: %v", err.(*discovery.ErrGroupDiscoveryFailed).Groups)
		} else {
			klog.Warningf("failed to discover preferred resources: %v", err)
		}
	}
	if preferredResources == nil {
		return map[schema.GroupVersionResource]struct{}{}
	}

	// This is extracted from discovery.GroupVersionResources to allow tolerating
	// failures on a per-resource basis.
	deletableResources := discovery.FilteredBy(discovery.SupportsAllVerbs{Verbs: []string{"delete", "list", "watch"}}, preferredResources)
	deletableGroupVersionResources := map[schema.GroupVersionResource]struct{}{}
	for _, rl := range deletableResources {
		gv, err := schema.ParseGroupVersion(rl.GroupVersion)
		if err != nil {
			klog.Warningf("ignoring invalid discovered resource %q: %v", rl.GroupVersion, err)
			continue
		}
		for i := range rl.APIResources {
			deletableGroupVersionResources[schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: rl.APIResources[i].Name}] = struct{}{}
		}
	}

	return deletableGroupVersionResources
}

func (gc *InUseProtector) runAddToProtectionWorker() {
	for gc.addToProtectionWorker() {
	}
}

func (gc *InUseProtector) addToProtectionWorker() bool {
	item, quit := gc.addToProtection.Get()
	gc.workerLock.RLock()
	defer gc.workerLock.RUnlock()
	if quit {
		return false
	}
	defer gc.addToProtection.Done(item)
	n, ok := item.(*node)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("expect *node, got %#v", item))
		return true
	}

	// Only protect node that is observed and has dependents
	if !n.isObserved() || n.dependentsLength() == 0 {
		return true
	}

	// Add FinalizerDeleteDependents finalizer
	if err := gc.addFinalizer(n, FinalizerDeleteDependents); err != nil {
		// retry on error
		gc.addToProtection.AddRateLimited(item)
		return true
	}

	return true
}

func (gc *InUseProtector) runRemoveFromProtectionWorker() {
	for gc.removeFromProtectionWorker() {
	}
}

func (gc *InUseProtector) removeFromProtectionWorker() bool {
	item, quit := gc.removeFromProtection.Get()
	gc.workerLock.RLock()
	defer gc.workerLock.RUnlock()
	if quit {
		return false
	}
	defer gc.removeFromProtection.Done(item)
	n, ok := item.(*node)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("expect *node, got %#v", item))
		return true
	}

	// Only remove protect from node that is observed and has no dependent
	if !n.isObserved() || n.dependentsLength() != 0 {
		return true
	}

	// Remove FinalizerDeleteDependents finalizer
	if err := gc.removeFinalizer(n, FinalizerDeleteDependents); err != nil {
		// retry on error
		gc.removeFromProtection.AddRateLimited(item)
		return true
	}

	return true
}
