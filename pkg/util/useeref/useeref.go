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

package useeref

import (
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var (
	useeReferenceKey = "k8s.io/useereference"
)

// UseeReference is a prototype implementation of usee reference as an annotation.
// TODO: this should be in metav1 and be implemented as fields.
type UseeReference struct {
	UID        types.UID
	Name       string
	Kind       string
	APIVersion string
}

// GetUseeRef returns a slice of UseeReference for the accessor
func GetUseeRef(accessor metav1.Object) []UseeReference {
	annotations := accessor.GetAnnotations()
	useeRefField, ok := annotations[useeReferenceKey]
	if !ok {
		return []UseeReference{}
	}

	return parseUsingRef(useeRefField)
}

func parseUsingRef(str string) []UseeReference {
	var useeRefs []UseeReference
	err := json.Unmarshal([]byte(trimSurroundingQuotes(str)), &useeRefs)
	if err != nil {
		// Not return error, instead return empty UseeReference
		return []UseeReference{}
	}

	return useeRefs
}

// https://stackoverflow.com/questions/48449224/remove-surrounding-double-or-single-quotes-in-golang
func trimSurroundingQuotes(s string) string {
	if len(s) >= 2 {
		if c := s[len(s)-1]; s[0] == c && (c == '"' || c == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
