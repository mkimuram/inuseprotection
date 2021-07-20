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

package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"time"

	"github.com/mkimuram/inuseprotection/pkg/controller"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	cacheddiscovery "k8s.io/client-go/discovery/cached"
	coreinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatainformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/controller-manager/pkg/informerfactory"
	"k8s.io/klog"
)

const (
	// Default timeout
	defaultTimeout = time.Minute
)

// Command line flags
var (
	kubeconfig   = flag.String("kubeconfig", "", "Absolute path to the kubeconfig file. Required only when running out of cluster.")
	timeout      = flag.Duration("timeout", defaultTimeout, "The timeout for the controller. Default is 1 minute.")
	resyncPeriod = flag.Duration("resync-period", 15*time.Minute, "Resync interval of the controller. Default is 15 minutes")
)

func main() {
	klog.InitFlags(nil)
	flag.Set("alsologtostderr", "true")
	flag.Parse()

	// Create the client config. Use kubeconfig if given, otherwise assume in-cluster.
	config, err := buildConfig(*kubeconfig)
	if err != nil {
		klog.Error(err.Error())
		os.Exit(1)
	}

	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Error(err.Error())
		os.Exit(1)
	}

	metadataClient, err := metadata.NewForConfig(config)
	if err != nil {
		klog.Error(err.Error())
		os.Exit(1)
	}

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		klog.Error(err.Error())
		os.Exit(1)
	}
	cachedClient := cacheddiscovery.NewMemCacheClient(discoveryClient)

	restMapper := restmapper.NewDeferredDiscoveryRESTMapper(cachedClient)
	ignoredResources := controller.DefaultIgnoredResources()

	sharedInformers := coreinformers.NewSharedInformerFactory(kubeClient, *resyncPeriod)
	metadataInformers := metadatainformer.NewSharedInformerFactory(metadataClient, *resyncPeriod)
	objectOrMetadataInformerFactory := informerfactory.NewInformerFactory(sharedInformers, metadataInformers)

	// Pass a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	stopCh := make(chan struct{})

	if err := startInUseProtector(ctx, kubeClient, metadataClient, discoveryClient, restMapper, ignoredResources, objectOrMetadataInformerFactory, stopCh); err != nil {
		klog.Error(err.Error())
		os.Exit(1)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	close(stopCh)
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func startInUseProtector(ctx context.Context, client kubernetes.Interface, metadataClient metadata.Interface, discoveryClient discovery.DiscoveryInterface, mapper controller.ResettableRESTMapper, ignoredResources map[schema.GroupResource]struct{}, informers informerfactory.InformerFactory, stopCh <-chan struct{}) error {

	informersStarted := make(chan struct{})

	ctrl, err := controller.NewInUseProtector(
		client,
		metadataClient,
		mapper,
		ignoredResources,
		informers,
		informersStarted,
	)
	if err != nil {
		return err
	}

	go ctrl.Run(1, stopCh)
	go ctrl.Sync(discoveryClient, 30*time.Second, stopCh)

	informers.Start(stopCh)
	close(informersStarted)

	return nil
}
