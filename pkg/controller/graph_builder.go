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
	"fmt"
	"reflect"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"github.com/mkimuram/inuseprotection/pkg/controller/metaonly"
	"github.com/mkimuram/inuseprotection/pkg/util/useeref"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/controller-manager/pkg/informerfactory"
)

const (
	FinalizerDeleteDependents = "k8s.io/in-use-protection"
)

type eventType int

func (e eventType) String() string {
	switch e {
	case addEvent:
		return "add"
	case updateEvent:
		return "update"
	case deleteEvent:
		return "delete"
	default:
		return fmt.Sprintf("unknown(%d)", int(e))
	}
}

const (
	addEvent eventType = iota
	updateEvent
	deleteEvent
)

type event struct {
	// virtual indicates this event did not come from an informer, but was constructed artificially
	virtual   bool
	eventType eventType
	obj       interface{}
	// the update event comes with an old object, but it's not used by the garbage collector.
	oldObj interface{}
	gvk    schema.GroupVersionKind
}

// GraphBuilder processes events supplied by the informers, updates uidToNode,
// a graph that caches the dependencies as we know, and enqueues
// items to the attemptToDelete.
type GraphBuilder struct {
	restMapper meta.RESTMapper

	// each monitor list/watches a resource, the results are funneled to the
	// dependencyGraphBuilder
	monitors    monitors
	monitorLock sync.RWMutex
	// informersStarted is closed after after all of the controllers have been initialized and are running.
	// After that it is safe to start them here, before that it is not.
	informersStarted <-chan struct{}

	// stopCh drives shutdown. When a receive from it unblocks, monitors will shut down.
	// This channel is also protected by monitorLock.
	stopCh <-chan struct{}

	// running tracks whether Run() has been called.
	// it is protected by monitorLock.
	running bool

	eventRecorder record.EventRecorder

	metadataClient metadata.Interface
	// monitors are the producer of the graphChanges queue, graphBuilder alters
	// the in-memory graph according to the changes.
	graphChanges workqueue.RateLimitingInterface
	// uidToNode doesn't require a lock to protect, because only the
	// single-threaded GraphBuilder.processGraphChanges() reads/writes it.
	uidToNode *concurrentUIDToNode
	// GraphBuilder is the producer of attemptToDelete, in-user protector is the consumer.
	attemptToDelete workqueue.RateLimitingInterface
	// GraphBuilder is the producer of addToProtection, in-user protector is the consumer.
	addToProtection workqueue.RateLimitingInterface
	// GraphBuilder is the producer of removeFromProtection, in-user protector is the consumer.
	removeFromProtection workqueue.RateLimitingInterface
	// GraphBuilder and GC share the absentUseeCache. Objects that are known to
	// be non-existent are added to the cached.
	absentUseeCache  *ReferenceCache
	sharedInformers  informerfactory.InformerFactory
	ignoredResources map[schema.GroupResource]struct{}
}

// monitor runs a Controller with a local stop channel.
type monitor struct {
	controller cache.Controller
	store      cache.Store

	// stopCh stops Controller. If stopCh is nil, the monitor is considered to be
	// not yet started.
	stopCh chan struct{}
}

// Run is intended to be called in a goroutine. Multiple calls of this is an
// error.
func (m *monitor) Run() {
	m.controller.Run(m.stopCh)
}

type monitors map[schema.GroupVersionResource]*monitor

func (gb *GraphBuilder) controllerFor(resource schema.GroupVersionResource, kind schema.GroupVersionKind) (cache.Controller, cache.Store, error) {
	handlers := cache.ResourceEventHandlerFuncs{
		// add the event to the dependencyGraphBuilder's graphChanges.
		AddFunc: func(obj interface{}) {
			event := &event{
				eventType: addEvent,
				obj:       obj,
				gvk:       kind,
			}
			gb.graphChanges.Add(event)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			// TODO: check if there are differences in the useeRefs,
			// finalizers, and DeletionTimestamp; if not, ignore the update.
			event := &event{
				eventType: updateEvent,
				obj:       newObj,
				oldObj:    oldObj,
				gvk:       kind,
			}
			gb.graphChanges.Add(event)
		},
		DeleteFunc: func(obj interface{}) {
			// delta fifo may wrap the object in a cache.DeletedFinalStateUnknown, unwrap it
			if deletedFinalStateUnknown, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = deletedFinalStateUnknown.Obj
			}
			event := &event{
				eventType: deleteEvent,
				obj:       obj,
				gvk:       kind,
			}
			gb.graphChanges.Add(event)
		},
	}
	shared, err := gb.sharedInformers.ForResource(resource)
	if err != nil {
		klog.V(4).Infof("unable to use a shared informer for resource %q, kind %q: %v", resource.String(), kind.String(), err)
		return nil, nil, err
	}
	klog.V(4).Infof("using a shared informer for resource %q, kind %q", resource.String(), kind.String())
	// need to clone because it's from a shared cache
	shared.Informer().AddEventHandlerWithResyncPeriod(handlers, ResourceResyncTime)
	return shared.Informer().GetController(), shared.Informer().GetStore(), nil
}

// syncMonitors rebuilds the monitor set according to the supplied resources,
// creating or deleting monitors as necessary. It will return any error
// encountered, but will make an attempt to create a monitor for each resource
// instead of immediately exiting on an error. It may be called before or after
// Run. Monitors are NOT started as part of the sync. To ensure all existing
// monitors are started, call startMonitors.
func (gb *GraphBuilder) syncMonitors(resources map[schema.GroupVersionResource]struct{}) error {
	gb.monitorLock.Lock()
	defer gb.monitorLock.Unlock()

	toRemove := gb.monitors
	if toRemove == nil {
		toRemove = monitors{}
	}
	current := monitors{}
	errs := []error{}
	kept := 0
	added := 0
	for resource := range resources {
		if _, ok := gb.ignoredResources[resource.GroupResource()]; ok {
			continue
		}
		if m, ok := toRemove[resource]; ok {
			current[resource] = m
			delete(toRemove, resource)
			kept++
			continue
		}
		kind, err := gb.restMapper.KindFor(resource)
		if err != nil {
			errs = append(errs, fmt.Errorf("couldn't look up resource %q: %v", resource, err))
			continue
		}
		c, s, err := gb.controllerFor(resource, kind)
		if err != nil {
			errs = append(errs, fmt.Errorf("couldn't start monitor for resource %q: %v", resource, err))
			continue
		}
		current[resource] = &monitor{store: s, controller: c}
		added++
	}
	gb.monitors = current

	for _, monitor := range toRemove {
		if monitor.stopCh != nil {
			close(monitor.stopCh)
		}
	}

	klog.V(4).Infof("synced monitors; added %d, kept %d, removed %d", added, kept, len(toRemove))
	// NewAggregate returns nil if errs is 0-length
	return utilerrors.NewAggregate(errs)
}

// startMonitors ensures the current set of monitors are running. Any newly
// started monitors will also cause shared informers to be started.
//
// If called before Run, startMonitors does nothing (as there is no stop channel
// to support monitor/informer execution).
func (gb *GraphBuilder) startMonitors() {
	gb.monitorLock.Lock()
	defer gb.monitorLock.Unlock()

	if !gb.running {
		return
	}

	// we're waiting until after the informer start that happens once all the controllers are initialized.  This ensures
	// that they don't get unexpected events on their work queues.
	<-gb.informersStarted

	monitors := gb.monitors
	started := 0
	for _, monitor := range monitors {
		if monitor.stopCh == nil {
			monitor.stopCh = make(chan struct{})
			gb.sharedInformers.Start(gb.stopCh)
			go monitor.Run()
			started++
		}
	}
	klog.V(4).Infof("started %d new monitors, %d currently running", started, len(monitors))
}

// IsSynced returns true if any monitors exist AND all those monitors'
// controllers HasSynced functions return true. This means IsSynced could return
// true at one time, and then later return false if all monitors were
// reconstructed.
func (gb *GraphBuilder) IsSynced() bool {
	gb.monitorLock.Lock()
	defer gb.monitorLock.Unlock()

	if len(gb.monitors) == 0 {
		klog.V(4).Info("garbage controller monitor not synced: no monitors")
		return false
	}

	for resource, monitor := range gb.monitors {
		if !monitor.controller.HasSynced() {
			klog.V(4).Infof("garbage controller monitor not yet synced: %+v", resource)
			return false
		}
	}
	return true
}

// Run sets the stop channel and starts monitor execution until stopCh is
// closed. Any running monitors will be stopped before Run returns.
func (gb *GraphBuilder) Run(stopCh <-chan struct{}) {
	klog.Infof("GraphBuilder running")
	defer klog.Infof("GraphBuilder stopping")

	// Set up the stop channel.
	gb.monitorLock.Lock()
	gb.stopCh = stopCh
	gb.running = true
	gb.monitorLock.Unlock()

	// Start monitors and begin change processing until the stop channel is
	// closed.
	gb.startMonitors()
	wait.Until(gb.runProcessGraphChanges, 1*time.Second, stopCh)

	// Stop any running monitors.
	gb.monitorLock.Lock()
	defer gb.monitorLock.Unlock()
	monitors := gb.monitors
	stopped := 0
	for _, monitor := range monitors {
		if monitor.stopCh != nil {
			stopped++
			close(monitor.stopCh)
		}
	}

	// reset monitors so that the graph builder can be safely re-run/synced.
	gb.monitors = nil
	klog.Infof("stopped %d of %d monitors", stopped, len(monitors))
}

var ignoredResources = map[schema.GroupResource]struct{}{
	{Group: "", Resource: "events"}: {},
}

// DefaultIgnoredResources returns the default set of resources that the garbage collector controller
// should ignore. This is exposed so downstream integrators can have access to the defaults, and add
// to them as necessary when constructing the controller.
func DefaultIgnoredResources() map[schema.GroupResource]struct{} {
	return ignoredResources
}

// enqueueVirtualDeleteEvent is used to add a virtual delete event to be processed for virtual nodes
// once it is determined they do not have backing objects in storage
func (gb *GraphBuilder) enqueueVirtualDeleteEvent(ref objectReference) {
	gv, _ := schema.ParseGroupVersion(ref.APIVersion)
	gb.graphChanges.Add(&event{
		virtual:   true,
		eventType: deleteEvent,
		gvk:       gv.WithKind(ref.Kind),
		obj: &metaonly.MetadataOnlyObject{
			TypeMeta:   metav1.TypeMeta{APIVersion: ref.APIVersion, Kind: ref.Kind},
			ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, UID: ref.UID, Name: ref.Name},
		},
	})
}

// addDependentToUsees adds n to usee's dependents list. If the usee does not
// exist in the gb.uidToNode yet, a "virtual" node will be created to represent
// the usee. The "virtual" node will be enqueued to the attemptToDelete, so that
// attemptToDeleteItem() will verify if the usee exists according to the API server.
func (gb *GraphBuilder) addDependentToUsees(n *node, usees []useeref.UseeReference) {
	// track if some of the referenced usees already exist in the graph and have been observed,
	// and the dependent's useeRef does not match their observed coordinates
	hasPotentiallyInvalidUseeReference := false

	for _, usee := range usees {
		useeNode, ok := gb.uidToNode.Read(usee.UID)
		if !ok {
			// Create a "virtual" node in the graph for the usee if it doesn't
			// exist in the graph yet.
			useeNode = &node{
				identity: objectReference{
					UseeReference: useeReferenceCoordinates(usee),
					Namespace:     n.identity.Namespace,
				},
				dependents: make(map[*node]struct{}),
				virtual:    true,
			}
			klog.V(5).Infof("add virtual node.identity: %s\n\n", useeNode.identity)
			gb.uidToNode.Write(useeNode)
		}
		useeNode.addDependent(n)
		if !useeNode.virtual {
			klog.Infof("add protection to %s/%s/%s used by %s/%s/%s", useeNode.identity.Kind, useeNode.identity.Namespace, useeNode.identity.Name, n.identity.Kind, n.identity.Namespace, n.identity.Name)
			gb.addToProtection.Add(useeNode)
		}
		if !ok {
			// Enqueue the virtual node into attemptToDelete.
			// The garbage processor will enqueue a virtual delete
			// event to delete it from the graph if API server confirms this
			// usee doesn't exist.
			gb.attemptToDelete.Add(useeNode)
		} else if !hasPotentiallyInvalidUseeReference {
			useeIsNamespaced := len(useeNode.identity.Namespace) > 0
			if useeIsNamespaced && useeNode.identity.Namespace != n.identity.Namespace {
				if useeNode.isObserved() {
					// The usee node has been observed via an informer
					// the dependent's namespace doesn't match the observed usee's namespace, this is definitely wrong.
					// cluster-scoped usees can be referenced as an usee from any namespace or cluster-scoped object.
					klog.V(2).Infof("node %s references an usee %s but does not match namespaces", n.identity, useeNode.identity)
					gb.reportInvalidNamespaceUseeRef(n, usee.UID)
				}
				hasPotentiallyInvalidUseeReference = true
			} else if !useeReferenceMatchesCoordinates(usee, useeNode.identity.UseeReference) {
				if useeNode.isObserved() {
					// The usee node has been observed via an informer
					// n's usee reference doesn't match the observed identity, this might be wrong.
					klog.V(2).Infof("node %s references an usee %s with coordinates that do not match the observed identity", n.identity, useeNode.identity)
				}
				hasPotentiallyInvalidUseeReference = true
			} else if !useeIsNamespaced && useeNode.identity.Namespace != n.identity.Namespace && !useeNode.isObserved() {
				// the useeNode is cluster-scoped and virtual, and does not match the child node's namespace.
				// the usee could be a missing instance of a namespaced type incorrectly referenced by a cluster-scoped child (issue #98040).
				// enqueue this child to attemptToDelete to verify parent references.
				hasPotentiallyInvalidUseeReference = true
			}
		}
	}

	if hasPotentiallyInvalidUseeReference {
		// Enqueue the potentially invalid dependent node into attemptToDelete.
		// The garbage processor will verify whether the usee references are dangling
		// and delete the dependent if all usee references are confirmed absent.
		gb.attemptToDelete.Add(n)
	}
}

func (gb *GraphBuilder) reportInvalidNamespaceUseeRef(n *node, invalidUseeUID types.UID) {
	var invalidUseeRef useeref.UseeReference
	var found = false
	for _, useeRef := range n.usees {
		if useeRef.UID == invalidUseeUID {
			invalidUseeRef = useeRef
			found = true
			break
		}
	}
	if !found {
		return
	}
	ref := &v1.ObjectReference{
		Kind:       n.identity.Kind,
		APIVersion: n.identity.APIVersion,
		Namespace:  n.identity.Namespace,
		Name:       n.identity.Name,
		UID:        n.identity.UID,
	}
	invalidIdentity := objectReference{
		UseeReference: useeref.UseeReference{
			Kind:       invalidUseeRef.Kind,
			APIVersion: invalidUseeRef.APIVersion,
			Name:       invalidUseeRef.Name,
			UID:        invalidUseeRef.UID,
		},
		Namespace: n.identity.Namespace,
	}
	gb.eventRecorder.Eventf(ref, v1.EventTypeWarning, "UseeRefInvalidNamespace", "useeRef %s does not exist in namespace %q", invalidIdentity, n.identity.Namespace)
}

// insertNode insert the node to gb.uidToNode; then it finds all usees as listed
// in n.usees, and adds the node to their dependents list.
func (gb *GraphBuilder) insertNode(n *node) {
	gb.uidToNode.Write(n)
	gb.addDependentToUsees(n, n.usees)
}

// removeDependentFromUsees remove n from usees' dependents list.
func (gb *GraphBuilder) removeDependentFromUsees(n *node, usees []useeref.UseeReference) {
	for _, usee := range usees {
		useeNode, ok := gb.uidToNode.Read(usee.UID)
		if !ok {
			continue
		}
		useeNode.deleteDependent(n)
		// if useeNode has no dependents now, finalizer should be removed
		if len(useeNode.getDependents()) == 0 {
			klog.Infof("remove protection from %s/%s/%s", useeNode.identity.Kind, useeNode.identity.Namespace, useeNode.identity.Name)
			gb.removeFromProtection.Add(useeNode)
		}
	}
}

// removeNode removes the node from gb.uidToNode, then finds all
// usees as listed in n.usees, and removes n from their dependents list.
func (gb *GraphBuilder) removeNode(n *node) {
	gb.uidToNode.Delete(n.identity.UID)
	gb.removeDependentFromUsees(n, n.usees)
}

type useeRefPair struct {
	oldRef useeref.UseeReference
	newRef useeref.UseeReference
}

// TODO: profile this function to see if a naive N^2 algorithm performs better
// when the number of references is small.
func referencesDiffs(old []useeref.UseeReference, new []useeref.UseeReference) (added []useeref.UseeReference, removed []useeref.UseeReference, changed []useeRefPair) {
	oldUIDToRef := make(map[string]useeref.UseeReference)
	for _, value := range old {
		oldUIDToRef[string(value.UID)] = value
	}
	oldUIDSet := sets.StringKeySet(oldUIDToRef)
	for _, value := range new {
		newUID := string(value.UID)
		if oldUIDSet.Has(newUID) {
			if !reflect.DeepEqual(oldUIDToRef[newUID], value) {
				changed = append(changed, useeRefPair{oldRef: oldUIDToRef[newUID], newRef: value})
			}
			oldUIDSet.Delete(newUID)
		} else {
			added = append(added, value)
		}
	}
	for oldUID := range oldUIDSet {
		removed = append(removed, oldUIDToRef[oldUID])
	}

	return added, removed, changed
}

func deletionStartsWithFinalizer(oldObj interface{}, newAccessor metav1.Object, matchingFinalizer string) bool {
	// if the new object isn't being deleted, or doesn't have the finalizer we're interested in, return false
	if !beingDeleted(newAccessor) || !hasFinalizer(newAccessor, matchingFinalizer) {
		return false
	}

	// if the old object is nil, or wasn't being deleted, or didn't have the finalizer, return true
	if oldObj == nil {
		return true
	}
	oldAccessor, err := meta.Accessor(oldObj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("cannot access oldObj: %v", err))
		return false
	}
	return !beingDeleted(oldAccessor) || !hasFinalizer(oldAccessor, matchingFinalizer)
}

func beingDeleted(accessor metav1.Object) bool {
	return accessor.GetDeletionTimestamp() != nil
}

func hasDeleteDependentsFinalizer(accessor metav1.Object) bool {
	return hasFinalizer(accessor, FinalizerDeleteDependents)
}

func hasFinalizer(accessor metav1.Object, matchingFinalizer string) bool {
	finalizers := accessor.GetFinalizers()
	for _, finalizer := range finalizers {
		if finalizer == matchingFinalizer {
			return true
		}
	}
	return false
}

// this function takes newAccessor directly because the caller already
// instantiates an accessor for the newObj.
func startsWaitingForDependentsDeleted(oldObj interface{}, newAccessor metav1.Object) bool {
	return deletionStartsWithFinalizer(oldObj, newAccessor, FinalizerDeleteDependents)
}

// if an blocking useeReference points to an object gets removed
// add the object to the attemptToDelete queue.
func (gb *GraphBuilder) addUnblockedUseesToDeleteQueue(removed []useeref.UseeReference, changed []useeRefPair) {
	for _, ref := range removed {
		node, found := gb.uidToNode.Read(ref.UID)
		if !found {
			klog.V(5).Infof("cannot find %s in uidToNode", ref.UID)
			continue
		}
		gb.attemptToDelete.Add(node)
	}
}

func (gb *GraphBuilder) processTransitions(oldObj interface{}, newAccessor metav1.Object, n *node) {
	if startsWaitingForDependentsDeleted(oldObj, newAccessor) {
		klog.V(2).Infof("add %s to the attemptToDelete, because it's waiting for its dependents to be deleted", n.identity)
		// if the n is added as a "virtual" node, its deletingDependents field is not properly set, so always set it here.
		n.markDeletingDependents()
		gb.attemptToDelete.Add(n)
	}
}

func (gb *GraphBuilder) runProcessGraphChanges() {
	for gb.processGraphChanges() {
	}
}

func identityFromEvent(event *event, accessor metav1.Object) objectReference {
	return objectReference{
		UseeReference: useeref.UseeReference{
			APIVersion: event.gvk.GroupVersion().String(),
			Kind:       event.gvk.Kind,
			UID:        accessor.GetUID(),
			Name:       accessor.GetName(),
		},
		Namespace: accessor.GetNamespace(),
	}
}

// Dequeueing an event from graphChanges, updating graph, populating dirty_queue.
func (gb *GraphBuilder) processGraphChanges() bool {
	item, quit := gb.graphChanges.Get()
	if quit {
		return false
	}
	defer gb.graphChanges.Done(item)
	event, ok := item.(*event)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("expect a *event, got %v", item))
		return true
	}
	obj := event.obj
	accessor, err := meta.Accessor(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("cannot access obj: %v", err))
		return true
	}
	klog.V(5).Infof("GraphBuilder process object: %s/%s, namespace %s, name %s, uid %s, event type %v, virtual=%v", event.gvk.GroupVersion().String(), event.gvk.Kind, accessor.GetNamespace(), accessor.GetName(), string(accessor.GetUID()), event.eventType, event.virtual)
	// Check if the node already exists
	existingNode, found := gb.uidToNode.Read(accessor.GetUID())
	if found && !event.virtual && !existingNode.isObserved() {
		// this marks the node as having been observed via an informer event
		// 1. this depends on graphChanges only containing add/update events from the actual informer
		// 2. this allows things tracking virtual nodes' existence to stop polling and rely on informer events
		observedIdentity := identityFromEvent(event, accessor)
		if observedIdentity != existingNode.identity {
			// find dependents that don't match the identity we observed
			_, potentiallyInvalidDependents := partitionDependents(existingNode.getDependents(), observedIdentity)
			// add those potentially invalid dependents to the attemptToDelete queue.
			// if their usees are still solid the attemptToDelete will be a no-op.
			// this covers the bad child -> good parent observation sequence.
			// the good parent -> bad child observation sequence is handled in addDependentToUsees
			for _, dep := range potentiallyInvalidDependents {
				if len(observedIdentity.Namespace) > 0 && dep.identity.Namespace != observedIdentity.Namespace {
					// Namespace mismatch, this is definitely wrong
					klog.V(2).Infof("node %s references a usee %s but does not match namespaces", dep.identity, observedIdentity)
					gb.reportInvalidNamespaceUseeRef(dep, observedIdentity.UID)
				}
				gb.attemptToDelete.Add(dep)
			}

			// make a copy (so we don't modify the existing node in place), store the observed identity, and replace the virtual node
			klog.V(2).Infof("replacing virtual node %s with observed node %s", existingNode.identity, observedIdentity)
			existingNode = existingNode.clone()
			existingNode.identity = observedIdentity
			gb.uidToNode.Write(existingNode)
		}
		existingNode.markObserved()

		// existingNode already has user before it is observed, so add to protect
		if dep := existingNode.getDependents(); len(dep) > 0 {
			d := dep[0]
			klog.Infof("add protection to %s/%s/%s used by %s/%s/%s, etc ...", existingNode.identity.Kind, existingNode.identity.Namespace, existingNode.identity.Name, d.identity.Kind, d.identity.Namespace, d.identity.Name)
			gb.addToProtection.Add(existingNode)
		}
	}
	switch {
	case (event.eventType == addEvent || event.eventType == updateEvent) && !found:
		newNode := &node{
			identity:           identityFromEvent(event, accessor),
			dependents:         make(map[*node]struct{}),
			usees:              useeref.GetUseeRef(accessor),
			deletingDependents: beingDeleted(accessor) && hasDeleteDependentsFinalizer(accessor),
			beingDeleted:       beingDeleted(accessor),
		}
		gb.insertNode(newNode)
		// the underlying delta_fifo may combine a creation and a deletion into
		// one event, so we need to further process the event.
		gb.processTransitions(event.oldObj, accessor, newNode)
	case (event.eventType == addEvent || event.eventType == updateEvent) && found:
		// handle changes in useeReferences
		added, removed, changed := referencesDiffs(existingNode.usees, useeref.GetUseeRef(accessor))
		if len(added) != 0 || len(removed) != 0 || len(changed) != 0 {
			// check if the changed dependency graph unblock usees that are
			// waiting for the deletion of their dependents.
			gb.addUnblockedUseesToDeleteQueue(removed, changed)
			// update the node itself
			existingNode.usees = useeref.GetUseeRef(accessor)
			// Add the node to its new usees' dependent lists.
			gb.addDependentToUsees(existingNode, added)
			// remove the node from the dependent list of node that are no longer in
			// the node's usees list.
			gb.removeDependentFromUsees(existingNode, removed)
		}

		if beingDeleted(accessor) {
			existingNode.markBeingDeleted()
		}
		gb.processTransitions(event.oldObj, accessor, existingNode)
	case event.eventType == deleteEvent:
		if !found {
			klog.V(5).Infof("%v doesn't exist in the graph, this shouldn't happen", accessor.GetUID())
			return true
		}

		removeExistingNode := true

		if event.virtual {
			// this is a virtual delete event, not one observed from an informer
			deletedIdentity := identityFromEvent(event, accessor)
			if existingNode.virtual {

				// our existing node is also virtual, we're not sure of its coordinates.
				// see if any dependents reference this usee with coordinates other than the one we got a virtual delete event for.
				if matchingDependents, nonmatchingDependents := partitionDependents(existingNode.getDependents(), deletedIdentity); len(nonmatchingDependents) > 0 {

					// some of our dependents disagree on our coordinates, so do not remove the existing virtual node from the graph
					removeExistingNode = false

					if len(matchingDependents) > 0 {
						// mark the observed deleted identity as absent
						gb.absentUseeCache.Add(deletedIdentity)
						// attempt to delete dependents that do match the verified deleted identity
						for _, dep := range matchingDependents {
							gb.attemptToDelete.Add(dep)
						}
					}

					// if the delete event verified existingNode.identity doesn't exist...
					if existingNode.identity == deletedIdentity {
						// find an alternative identity our nonmatching dependents refer to us by
						replacementIdentity := getAlternateUseeIdentity(nonmatchingDependents, deletedIdentity)
						if replacementIdentity != nil {
							// replace the existing virtual node with a new one with one of our other potential identities
							replacementNode := existingNode.clone()
							replacementNode.identity = *replacementIdentity
							gb.uidToNode.Write(replacementNode)
							// and add the new virtual node back to the attemptToDelete queue
							gb.attemptToDelete.AddRateLimited(replacementNode)
						}
					}
				}

			} else if existingNode.identity != deletedIdentity {
				// do not remove the existing real node from the graph based on a virtual delete event
				removeExistingNode = false

				// our existing node which was observed via informer disagrees with the virtual delete event's coordinates
				matchingDependents, _ := partitionDependents(existingNode.getDependents(), deletedIdentity)

				if len(matchingDependents) > 0 {
					// mark the observed deleted identity as absent
					gb.absentUseeCache.Add(deletedIdentity)
					// attempt to delete dependents that do match the verified deleted identity
					for _, dep := range matchingDependents {
						gb.attemptToDelete.Add(dep)
					}
				}
			}
		}

		if removeExistingNode {
			// removeNode updates the graph
			gb.removeNode(existingNode)
			existingNode.dependentsLock.RLock()
			defer existingNode.dependentsLock.RUnlock()
			if len(existingNode.dependents) > 0 {
				gb.absentUseeCache.Add(identityFromEvent(event, accessor))
			}
		}
	}
	return true
}

// partitionDependents divides the provided dependents into a list which have a useeReference matching the provided identity,
// and ones which have a useeReference for the given uid that do not match the provided identity.
// Note that a dependent with multiple useeReferences for the target uid can end up in both lists.
func partitionDependents(dependents []*node, matchUseeIdentity objectReference) (matching, nonmatching []*node) {
	useeIsNamespaced := len(matchUseeIdentity.Namespace) > 0
	for i := range dependents {
		dep := dependents[i]
		foundMatch := false
		foundMismatch := false
		// if the dep namespace matches or the usee is cluster scoped ...
		if useeIsNamespaced && matchUseeIdentity.Namespace != dep.identity.Namespace {
			// all references to the parent do not match, since the dependent namespace does not match the usee
			foundMismatch = true
		} else {
			for _, useeRef := range dep.usees {
				// ... find the useeRef with a matching uid ...
				if useeRef.UID == matchUseeIdentity.UID {
					// ... and check if it matches all coordinates
					if useeReferenceMatchesCoordinates(useeRef, matchUseeIdentity.UseeReference) {
						foundMatch = true
					} else {
						foundMismatch = true
					}
				}
			}
		}

		if foundMatch {
			matching = append(matching, dep)
		}
		if foundMismatch {
			nonmatching = append(nonmatching, dep)
		}
	}
	return matching, nonmatching
}

func referenceLessThan(a, b objectReference) bool {
	// kind/apiVersion are more significant than namespace,
	// so that we get coherent ordering between kinds
	// regardless of whether they are cluster-scoped or namespaced
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	if a.APIVersion != b.APIVersion {
		return a.APIVersion < b.APIVersion
	}
	// namespace is more significant than name
	if a.Namespace != b.Namespace {
		return a.Namespace < b.Namespace
	}
	// name is more significant than uid
	if a.Name != b.Name {
		return a.Name < b.Name
	}
	// uid is included for completeness, but is expected to be identical
	// when getting alternate identities for an usee since they are keyed by uid
	if a.UID != b.UID {
		return a.UID < b.UID
	}
	return false
}

// getAlternateUseeIdentity searches deps for usee references which match
// verifiedAbsentIdentity.UID but differ in apiVersion/kind/name or namespace.
// The first that follows verifiedAbsentIdentity (according to referenceLessThan) is returned.
// If none follow verifiedAbsentIdentity, the first (according to referenceLessThan) is returned.
// If no alternate identities are found, nil is returned.
func getAlternateUseeIdentity(deps []*node, verifiedAbsentIdentity objectReference) *objectReference {
	absentIdentityIsClusterScoped := len(verifiedAbsentIdentity.Namespace) == 0

	seenAlternates := map[objectReference]bool{verifiedAbsentIdentity: true}

	// keep track of the first alternate reference (according to referenceLessThan)
	var first *objectReference
	// keep track of the first reference following verifiedAbsentIdentity (according to referenceLessThan)
	var firstFollowing *objectReference

	for _, dep := range deps {
		for _, useeRef := range dep.usees {
			if useeRef.UID != verifiedAbsentIdentity.UID {
				// skip references that aren't the uid we care about
				continue
			}

			if useeReferenceMatchesCoordinates(useeRef, verifiedAbsentIdentity.UseeReference) {
				if absentIdentityIsClusterScoped || verifiedAbsentIdentity.Namespace == dep.identity.Namespace {
					// skip references that exactly match verifiedAbsentIdentity
					continue
				}
			}

			ref := objectReference{UseeReference: useeReferenceCoordinates(useeRef), Namespace: dep.identity.Namespace}
			if absentIdentityIsClusterScoped && ref.APIVersion == verifiedAbsentIdentity.APIVersion && ref.Kind == verifiedAbsentIdentity.Kind {
				// we know this apiVersion/kind is cluster-scoped because of verifiedAbsentIdentity,
				// so clear the namespace from the alternate identity
				ref.Namespace = ""
			}

			if seenAlternates[ref] {
				// skip references we've already seen
				continue
			}
			seenAlternates[ref] = true

			if first == nil || referenceLessThan(ref, *first) {
				// this alternate comes first lexically
				first = &ref
			}
			if referenceLessThan(verifiedAbsentIdentity, ref) && (firstFollowing == nil || referenceLessThan(ref, *firstFollowing)) {
				// this alternate is the first following verifiedAbsentIdentity lexically
				firstFollowing = &ref
			}
		}
	}

	// return the first alternate identity following the verified absent identity, if there is one
	if firstFollowing != nil {
		return firstFollowing
	}
	// otherwise return the first alternate identity
	return first
}
