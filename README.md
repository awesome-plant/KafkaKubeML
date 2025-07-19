preflight 
- helmfile is used on windows
```scoop install helmfile```

kube instructions 
```
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
