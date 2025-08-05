# Full-Stack Data Platform on Kubernetes

This project demonstrates a full-stack, containerized data platform using Kubernetes as the orchestration layer and Airflow for workflow management. It simulates user activity, ingests and streams data, and applies machine learning models for inference, with plans for a frontend UI.

---

## 🏗️ Architecture & Components

1. **Kubernetes (k8s)**
    - Orchestrates all containers and services.
    - Provides scalability and common configuration using Helm and Helmfile.

2. **Airflow**
    - Orchestrates end-to-end workflows.
    - Schedules and manages pipeline tasks from generation to inference.

3. **Python User Interaction Generator**
    - Simulates fake user events.
    - Sends generated events to a streaming layer (Kafka).

4. **Kafka**
    - Provides real-time data streaming and transport.
    - Receives messages from the event generator and makes them available to consumers.

5. **Python Kafka Consumer**
    - Consumes events from Kafka.
    - Writes events to Parquet files in a Kubernetes Persistent Volume Claim (PVC).

6. **DuckDB Integration** *(optional / for future)*
    - (Planned) Enables SQL queries directly on parquet data.

7. **Python ML Worker (scikit-learn)**
    - Reads Parquet data.
    - Runs scikit-learn models for ML inference or training.

8. **FastAPI Inference Server**
    - Serves real-time inference via REST API endpoints.

9. **User Interface (UI)**
    - (Planned) For event visualization and analytics.

---

## 🚀 Deployment

- **Helmfile** manages all Helm-based Kubernetes deployments, ensuring common variables and configurations.

---

## ⚡ Getting Started

1. Clone this repository.
2. Install [Helm](https://helm.sh/) and [Helmfile](https://github.com/roboll/helmfile).
3. Deploy the stack:
    ```sh
    kubectl create namespace kafka-kube-ml
    kubectl config set-context --current --namespace=kafka-kube-ml
    helmfile apply
    ```
4. Access Airflow, Kafka, and FastAPI endpoints as defined by your Kubernetes services.

## ⚡ Resetting Kube instance 

1. run code below
  ```bash
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
  kubectl delete pvc consumer-parquet-pvc
  kubectl delete pv consumer-parquet-pv
  ```
---

## 📁 Project Structure

```
.
├── dags/
│   ├── dummy_dag.py
│   ├── python_worker.py
│   ├── requirements.txt
│   ├── streaming_ai_pipeline.py
│   ├── __init__.py
│   └── __pycache__/
│       ├── dummy_dag.cpython-312.pyc
│       ├── python_worker.cpython-312.pyc
│       ├── streaming_ai_pipeline.cpython-312.pyc
│       └── __init__.cpython-312.pyc
├── helmfile.gotmpl
├── helmfile.yaml
├── k8s/
│   ├── airflow/
│   │   ├── airflow-dags-pvc.yaml
│   │   ├── airflow-ingress.yaml
│   │   └── values.yaml
│   ├── dashboard/
│   │   └── recommended.yaml
│   ├── ingress-nginx/
│   │   └── values.yaml
│   ├── kafka/
│   │   ├── kafka-cluster.yaml
│   │   └── strimzi-crds.yaml
│   └── kafka-namespace.yaml
├── README.md
├── stream-processor/
│   └── Dockerfile
├── user-simulator/
│   ├── .dockerignore
│   ├── Dockerfile
│   ├── generate.py
│   ├── pyproject.toml
│   ├── README.md
│   ├── requirements.txt
│   ├── src/
│   │   └── user_simulator/
│   │       ├── generator.py
│   │       ├── kafka_client.py
│   │       ├── main.py
│   │       └── __init__.py
│   └── tests/
│       ├── integration/
│       │   └── test_kafka_end_to_end.py
│       ├── test_generator.py
│       ├── test_kafka_client.py
│       ├── test_main.py
│       └── __init__.py

```

---

## 📝 Notes

- DuckDB integration is a planned feature and not yet included.
- The UI is a placeholder for future releases.

---

## 🤝 Contributing

Questions or contributions? Please open an issue or pull request.

---
