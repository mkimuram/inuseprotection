# in-use protection

## Overview
The in-use-protector is an external controller that monitors objects' `useeReference` field and blocks deletion of the used objects while they are in-use.
This controller is intended to be used with other controllers that maintain the `useeReference` fields by detecting the user-usee relationships between specific objects with their domain specific knowledges.
An example of such controllers is [secret-protection controller](https://github.com/mkimuram/secret-protection).

Most of the codes in this repo are modified and copied from [garbagecollector](https://github.com/kubernetes/kubernetes/tree/master/pkg/controller/garbagecollector). 
The `useeReference` field is currently implemented as `k8s.io/useereference` [annotation](https://github.com/mkimuram/inuseprotection/blob/master/pkg/util/useeref/useeref.go#L32), just for a quick prototype.
However, it should be implemented as object's metadata fields in the same way that [`ownerReference`](https://github.com/kubernetes/apimachinery/blob/master/pkg/apis/meta/v1/types.go#L307) is implemented.

## Feature status
This project is still pre-alpha. This is just a prototype for discussion purpose. 

## Usage
### Build container 
```
make container-in-use-protector
```

### Deploy 
(Above container image needs to be available before below command, such as by running `kind load docker-image`.)
```
kubectl create -f examples/kubernetes/rbac-in-use-protector.yaml
kubectl create -f examples/kubernetes/setup-in-use-protector.yaml
```

### Undeploy
```
kubectl delete -f examples/kubernetes/setup-in-use-protector.yaml
kubectl delete -f examples/kubernetes/rbac-in-use-protector.yaml
```

While this controller is deployed, `k8s.io/in-use-protection` finalizers are added to resources that were referenced via `useeReference`, these finalizers are needed to be manually deleted after the controller is undeployed. 

For example, to remove finalizers from test-secret secret, run below comand.

```
kubectl patch secret test-secret -p '{"metadata":{"finalizers":[]}}' --type=merge
```

## How to test manually
1. Create a Secret and check that there is no finalizers added
```
kubectl create secret generic test-secret --from-literal='username=my-app' --from-literal='password=39528$vdg7Jb'
kubectl get secret test-secret -o jsonpath='{.metadata.finalizers}{"\n"}'
```

2. Create a Pod that has `useeReference` to the secret
```
uid=$(kubectl get secret test-secret -o jsonpath='{.metadata.uid}')

cat << EOF | kubectl create -f -
apiVersion: v1
kind: Pod
metadata:
  name: user-pod
  annotations:
    k8s.io/useereference: '[{"apiVersion": "core/v1", "kind": "Secret", "name": "test-secret", "uid": "$uid"}]'
spec:
  containers:
  - name: nginx
    image: nginx:1.14.2
EOF
```

As the above example shows, in-use protector doesn't care if the resource is actually using the resource that `useeReference` defines.
It is completely up to users or controllers to ensure that references reflect the real relationships.

3. Check that the `k8s.io/useereference` finalizer is added to test-secret by the in-use protector and `deletionTimestamp` is empty.
```
kubectl get secret test-secret -o jsonpath='{.metadata.finalizers}{"\n"}'
```
`[k8s.io/in-use-protection]` is shown.
```
kubectl get secret test-secret -o jsonpath='{.metadata.deletionTimestamp}{"\n"}'
```
Nothing is shown.

4. Try to delete the secret while it is in-use and check that it remains with the finalizer and non-empty `deletionTimestamp`
```
kubectl delete secret test-secret --wait=false
kubectl get secret test-secret -o jsonpath='{.metadata.finalizers}{"\n"}'
```
`[k8s.io/in-use-protection]` is shown.
```
kubectl get secret test-secret -o jsonpath='{.metadata.deletionTimestamp}{"\n"}'
```
Something like `2021-07-22T17:53:00Z` is shown.

5. Delete the user-pod and confirm that the secret is also deleted.
```
kubectl delete pod user-pod
```
`pod "user-pod" deleted` is shown.
```
kubectl get secret test-secret -o jsonpath='{.metadata.finalizers}{"\n"}'
```
`Error from server (NotFound): secrets "test-secret" not found` is shown.
