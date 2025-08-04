preflight 
- helmfile is used on windows
```scoop install helmfile```

kube instructions 
```
wsl -d Ubuntu
<!-- kubectl label node win10-agent-5609d4cd node-role.kubernetes.io/workload=true
kubectl label node win11 node-role.kubernetes.io/dashboard=true
kubectl label node win10-agent-5609d4cd airflow-role=worker
kubectl label node win11            dashboard-role=control -->
kubectl create namespace kafka-kube-ml
kubectl config set-context --current --namespace=kafka-kube-ml
```
--use helm--
apply helm common configs
```
helmfile apply
#helmfile -f helmfile.gotmpl -e default apply
```

delete at end 
```
#helmfile -f helmfile.gotmpl -e default destroy
helmfile destroy
```

hard reset
```

helmfile destroy
for ns in kafka kafka-kube-ml kafka-stream; do
  echo "Cleaning namespace: $ns"
  kubectl api-resources \
    --verbs=list \
    --namespaced \
    -o name \
  | xargs -r -n1 -I{} kubectl delete {} --all -n $ns
done

kubectl get ns \
--no-headers \
-o custom-columns=NAME:.metadata.name,STATUS:.status.phase \
| awk '$2=="Terminating"{print $1}' \
| xargs -r -n1 kubectl patch ns --type=merge -p '{"metadata":{"finalizers":[]}}'

helm uninstall ingress -n kafka-kube-ml
kubectl delete validatingwebhookconfiguration ingress-nginx-admission
kubectl delete clusterrolebinding admin-user
kubectl delete pv airflow-dags-pv
kubectl delete pvc airflow-dags-pvc

```

build dockerfiles
```
docker build -t kafkakubeml:user_simulator .
```