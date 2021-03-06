/*
Copyright 2016 The Kubernetes Authors.

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
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

// cluster scoped resources don't have namespaces.  Default to the item's namespace, but clear it for cluster scoped resources
func resourceDefaultNamespace(namespaced bool, defaultNamespace string) string {
	if namespaced {
		return defaultNamespace
	}
	return ""
}

// apiResource consults the REST mapper to translate an <apiVersion, kind,
// namespace> tuple to a unversioned.APIResource struct.
func (gc *InUseProtector) apiResource(apiVersion, kind string) (schema.GroupVersionResource, bool, error) {
	fqKind := schema.FromAPIVersionAndKind(apiVersion, kind)
	mapping, err := gc.restMapper.RESTMapping(fqKind.GroupKind(), fqKind.Version)
	if err != nil {
		return schema.GroupVersionResource{}, false, newRESTMappingError(kind, apiVersion)
	}
	return mapping.Resource, mapping.Scope == meta.RESTScopeNamespace, nil
}

func (gc *InUseProtector) getObject(item objectReference) (*metav1.PartialObjectMetadata, error) {
	resource, namespaced, err := gc.apiResource(item.APIVersion, item.Kind)
	if err != nil {
		return nil, err
	}
	namespace := resourceDefaultNamespace(namespaced, item.Namespace)
	if namespaced && len(namespace) == 0 {
		// the type is namespaced, but we have no namespace coordinate.
		// the only way this can happen is if a cluster-scoped object referenced this type as an usee.
		return nil, namespacedUseeOfClusterScopedObjectErr
	}
	return gc.metadataClient.Resource(resource).Namespace(namespace).Get(context.TODO(), item.Name, metav1.GetOptions{})
}

func (gc *InUseProtector) patchObject(item objectReference, patch []byte, pt types.PatchType) (*metav1.PartialObjectMetadata, error) {
	resource, namespaced, err := gc.apiResource(item.APIVersion, item.Kind)
	if err != nil {
		return nil, err
	}
	return gc.metadataClient.Resource(resource).Namespace(resourceDefaultNamespace(namespaced, item.Namespace)).Patch(context.TODO(), item.Name, pt, patch, metav1.PatchOptions{})
}

func (gc *InUseProtector) removeFinalizer(usee *node, targetFinalizer string) error {
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		useeObject, err := gc.getObject(usee.identity)
		if errors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("cannot finalize usee %s, because cannot get it: %v. The garbage collector will retry later", usee.identity, err)
		}
		accessor, err := meta.Accessor(useeObject)
		if err != nil {
			return fmt.Errorf("cannot access the usee object %v: %v. The garbage collector will retry later", useeObject, err)
		}
		finalizers := accessor.GetFinalizers()
		var newFinalizers []string
		found := false
		for _, f := range finalizers {
			if f == targetFinalizer {
				found = true
				continue
			}
			newFinalizers = append(newFinalizers, f)
		}
		if !found {
			klog.V(5).Infof("the %s finalizer is already removed from object %s", targetFinalizer, usee.identity)
			return nil
		}

		// remove the usee from dependent's UseeReferences
		patch, err := json.Marshal(&objectForFinalizersPatch{
			ObjectMetaForFinalizersPatch: ObjectMetaForFinalizersPatch{
				ResourceVersion: accessor.GetResourceVersion(),
				Finalizers:      newFinalizers,
			},
		})
		if err != nil {
			return fmt.Errorf("unable to finalize %s due to an error serializing patch: %v", usee.identity, err)
		}
		_, err = gc.patchObject(usee.identity, patch, types.MergePatchType)
		return err
	})
	if errors.IsConflict(err) {
		return fmt.Errorf("updateMaxRetries(%d) has reached. The garbage collector will retry later for usee %v", retry.DefaultBackoff.Steps, usee.identity)
	}
	return err
}

func (gc *InUseProtector) addFinalizer(usee *node, targetFinalizer string) error {
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		useeObject, err := gc.getObject(usee.identity)
		if errors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("cannot finalize usee %s, because cannot get it: %v. The garbage collector will retry later", usee.identity, err)
		}
		accessor, err := meta.Accessor(useeObject)
		if err != nil {
			return fmt.Errorf("cannot access the usee object %v: %v. The garbage collector will retry later", useeObject, err)
		}
		finalizers := accessor.GetFinalizers()
		var newFinalizers []string
		found := false
		for _, f := range finalizers {
			if f == targetFinalizer {
				found = true
				continue
			}
			newFinalizers = append(newFinalizers, f)
		}
		if found {
			klog.V(5).Infof("the %s finalizer is already added to object %s", targetFinalizer, usee.identity)
			return nil
		}

		newFinalizers = append(newFinalizers, targetFinalizer)

		// remove the usee from dependent's UseeReferences
		patch, err := json.Marshal(&objectForFinalizersPatch{
			ObjectMetaForFinalizersPatch: ObjectMetaForFinalizersPatch{
				ResourceVersion: accessor.GetResourceVersion(),
				Finalizers:      newFinalizers,
			},
		})
		if err != nil {
			return fmt.Errorf("unable to finalize %s due to an error serializing patch: %v", usee.identity, err)
		}
		_, err = gc.patchObject(usee.identity, patch, types.MergePatchType)
		return err
	})
	if errors.IsConflict(err) {
		return fmt.Errorf("updateMaxRetries(%d) has reached. The garbage collector will retry later for usee %v", retry.DefaultBackoff.Steps, usee.identity)
	}
	return err
}
