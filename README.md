# Laboratório: Migração Brownfield para GitOps — Streams for Apache Kafka 3.2 no OpenShift 4.20

Este repositório documenta o processo completo de migração de uma infraestrutura existente de mensageria baseada em **Streams for Apache Kafka 3.2** (Strimzi) no **Red Hat OpenShift 4.20**, instalada inicialmente de forma imperativa (`oc apply`), para uma gestão declarativa contínua via **OpenShift GitOps (Argo CD)** com zero downtime (*impacto zero*).

---

## 1. Visão Geral e Arquitetura

O objetivo central foi comprovar que o Argo CD pode assumir a custódia (*adoption*) de recursos já existentes sem forçar a recriação de objetos, sem causar reinício de pods (*zero-restart*) e sem perda de dados ou interrupção de tópicos Kafka.

### Especificações Técnicas do Ambiente
* **Cluster:** Red Hat OpenShift Container Platform `4.20`
* **Operador:** Red Hat Streams for Apache Kafka `3.2.2-5` (`amqstreams.v3.2.2-5`)
* **Motor Kafka:** Apache Kafka `4.2.0` (Metadata Version: `4.2-IV0`)
* **Modo de Operação:** KRaft nativo (*Kafka Raft metadata mode*, sem ZooKeeper) via `KafkaNodePool`
* **Versão das APIs:** `kafka.strimzi.io/v1`
* **Gerenciador GitOps:** Red Hat OpenShift GitOps (Argo CD `v3.5.5+a5038b0`)

---

## 2. Estrutura do Repositório

O repositório foi organizado separando os ciclos de vida do operador, do cluster de dados e da orquestração GitOps:

```text
.
├── README.md
└── infra/
    ├── argocd/
    │   ├── operator/
    │   │   ├── 01-namespace.yaml
    │   │   ├── 02-operatorgroup.yaml
    │   │   └── 03-subscription.yaml
    │   └── apps/
    │       └── applicationset.yaml
    └── kafka/
        ├── cluster/
        │   ├── 01-namespace.yaml
        │   ├── 02-kafka.yaml
        │   ├── 03-nodepool.yaml
        │   └── 04-topic.yaml
        └── operator/
            ├── 01-namespace.yaml
            ├── 02-operatorgroup.yaml
            └── 03-subscription.yaml
```
---

## 3. Passo a Passo Detalhado da Execução

### Fase 1: Instalação Manual do Operador Streams for Apache Kafka

Inicialmente, simulamos o ambiente *brownfield* (pré-existente) instalando o operador via CLI `oc`.

#### 1. Manifestos do Operador
* `infra/kafka/operator/01-namespace.yaml`: Criação do namespace `amq-streams-operator`.
* `infra/kafka/operator/02-operatorgroup.yaml`: 
  > **Nota de Arquitetura:** O `OperatorGroup` deve ser configurado com `spec: {}` (modo cluster-wide) para que o operador possa monitorar instâncias Kafka criadas em outros namespaces (como `kafka-cluster`).
* `infra/kafka/operator/03-subscription.yaml`: Inscrição no catálogo `redhat-operators`.

#### 2. Aplicação e Validação
```bash
oc apply -f infra/kafka/operator/
```

Validação do ClusterServiceVersion (CSV) e InstallPlan:
```bash
$ oc project amq-streams-operator
Now using project "amq-streams-operator" on server "https://api.ocp4lab.imesquit.tamlab.rdu2.redhat.com:6443".

$ oc get csv
NAME                  DISPLAY                    VERSION   REPLACES               PHASE
amqstreams.v3.2.2-5   Streams for Apache Kafka   3.2.2-5   amqstreams.v3.2.1-14   Succeeded

$ oc get installplan
NAME            CSV                   APPROVAL    APPROVED
install-gf5jz   amqstreams.v3.2.2-5   Automatic   true
```

![](./images/01%20-%20Installed%20Operator.png)

![](./images/02%20-%20Kafka%20Operator%20Details.png)

---

### Fase 2: Implantação Manual do Cluster KRaft (v1)

No Streams for Apache Kafka 3.2, o ZooKeeper foi descontinuado. A arquitetura passa a exigir KRaft nativo, o uso do Custom Resource `KafkaNodePool` e as definições sob a API estável `kafka.strimzi.io/v1`.

#### 1. Manifestos do Cluster
* `infra/kafka/cluster/01-namespace.yaml`: Namespace dedicado `kafka-cluster`.
* `infra/kafka/cluster/02-kafka.yaml`: CR `Kafka` v1 declarando versão `4.2.0`, `metadataVersion: 4.2-IV0` e anotações KRaft.
* `infra/kafka/cluster/03-nodepool.yaml`: CR `KafkaNodePool` v1 configurando 3 nós com papéis combinados (`controller` e `broker`).
* `infra/kafka/cluster/04-topic.yaml`: Tópico de validação `telemetry-events` (3 partições, fator de replicação 3).

#### 2. Aplicação e Validação
```bash
oc apply -f infra/kafka/cluster/
```

Validação de integridade dos nós KRaft e tópicos:
```bash
$ oc project kafka-cluster
Now using project "kafka-cluster" on server "https://api.ocp4lab.imesquit.tamlab.rdu2.redhat.com:6443".

$ oc get pods
NAME                                           READY   STATUS    RESTARTS   AGE
lab-cluster-dual-role-0                        1/1     Running   0          85s
lab-cluster-dual-role-1                        1/1     Running   0          85s
lab-cluster-dual-role-2                        1/1     Running   0          85s
lab-cluster-entity-operator-6966bfbfb5-75kvn   2/2     Running   0          49s

$ oc get kafkanodepools
NAME        DESIRED REPLICAS   ROLES                     NODEIDS
dual-role   3                  ["controller","broker"]   [0,1,2]

$ oc get kafka
NAME          READY   WARNINGS   KAFKA VERSION   METADATA VERSION
lab-cluster   True               4.2.0           4.2-IV0

$ oc get kafkatopic
NAME               CLUSTER       PARTITIONS   REPLICATION FACTOR   READY
telemetry-events   lab-cluster   3            3                    True
```

---

### Fase 3: Instalação Manual do Operador OpenShift GitOps
Antes de delegar o controle declarativo, provisionamos manualmente o operador do Red Hat OpenShift GitOps via OLM.

1. Manifesto de Inscrição (infra/argocd/operator/01-subscription.yaml)

O OpenShift GitOps Operator é implantado no namespace de operadores padrão (openshift-operators) com canal padrão estável, provisionando automaticamente o namespace openshift-gitops e a instância cluster do Argo CD:
```yaml
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: openshift-gitops-operator
  namespace: openshift-operators
spec:
  channel: latest
  installPlanApproval: Automatic
  name: openshift-gitops-operator
  source: redhat-operators
  sourceNamespace: openshift-marketplace
```

2. Aplicação e Validação

```bash
oc apply -f infra/argocd/operator/01-subscription.yaml
```

Acompanhe a instalação e criação automática dos componentes no namespace openshift-gitops:

```bash
# Valide o CSV no namespace openshift-operators
oc get csv -n openshift-operators -l operators.coreos.com/openshift-gitops-operator.openshift-operators

# Aguarde os pods do plano de controle do Argo CD atingirem status Ready
oc wait --for=condition=Ready pods --all -n openshift-gitops --timeout=300s

# Verifique as rotas geradas para o console web do Argo CD
oc get route openshift-gitops-server -n openshift-gitops
```

![](./images/03%20-%20OpenShift%20GitOps%20Installed.png)
![](./images/04%20-%20ArgoCD%20Operator%20Details.png)
![](./images/05%20-%20ArgoCD%20Login%20Page.png)
![](./images/06%20-%20ArgoCD%20Login%20Permissions.png)

### Fase 4: Conexão Segura ao Repositório Privado no Argo CD

Como os manifestos foram versionados em repositório Git privado, configuramos a credencial de acesso diretamente no OpenShift GitOps.

#### 1. Criação e Rotulação da Secret
```bash
oc create secret generic repo-kafka-gitops \
  -n openshift-gitops \
  --from-literal=type=git \
  --from-literal=url=https://github.com/rh-imesquit/ocp-kafka-gitops-repo \
  --from-literal=username=rh-imesquit \
  --from-literal=password=<SEU_TOKEN_PAT>

# Rótulo mandatório para o Argo CD indexar a secret
oc label secret repo-kafka-gitops -n openshift-gitops argocd.argoproj.io/secret-type=repository --overwrite
```
![](./images/08%20-%20ArgoCD%20Connected%20Repositories.png)


#### 2. Recarregamento de Cache
```bash
oc rollout restart deployment/openshift-gitops-server -n openshift-gitops
oc rollout restart deployment/openshift-gitops-repo-server -n openshift-gitops
```
*Status na UI:* A aba **Settings** → **Repositories** validou a conexão como **Successful**.

---

### Fase 5: Configuração de RBAC para o Argo CD

Por padrão, a ServiceAccount do controlador do Argo CD não possui autorização administrativa em namespaces de aplicação criados manualmente.

Sem este ajuste, o Argo CD falha ao tentar aplicar patches com a mensagem:
`cannot patch resource "kafkanodepools" in API group "kafka.strimzi.io" in the namespace "kafka-cluster": User ... is forbidden`.

#### Concessão de Privilégios:
```bash
oc adm policy add-role-to-user admin \
  system:serviceaccount:openshift-gitops:openshift-gitops-argocd-application-controller \
  -n kafka-cluster

oc adm policy add-role-to-user admin \
  system:serviceaccount:openshift-gitops:openshift-gitops-argocd-application-controller \
  -n amq-streams-operator
```

---

### Fase 5: Adoção GitOps via ApplicationSet

Criamos o `ApplicationSet` gerando duas instâncias de `Application`: uma para o operador e outra para o cluster Kafka.

#### Manifesto: `infra/argocd/apps/applicationset.yaml`
```yaml
apiVersion: argoproj.io/v1alpha1
kind: ApplicationSet
metadata:
  name: kafka-stack
  namespace: openshift-gitops
spec:
  generators:
    - list:
        elements:
          - name: kafka-operator
            path: infra/kafka/operator
            destNamespace: amq-streams-operator
          - name: kafka-cluster
            path: infra/kafka/cluster
            destNamespace: kafka-cluster
  template:
    metadata:
      name: '{{name}}'
    spec:
      project: default
      source:
        repoURL: https://github.com/rh-imesquit/ocp-kafka-gitops-repo
        targetRevision: main
        path: '{{path}}'
      destination:
        server: https://kubernetes.default.svc
        namespace: '{{destNamespace}}'
      syncPolicy:
        automated:
          prune: false          # Salvaguarda: impede exclusão de recursos legados
          selfHeal: true
        syncOptions:
          - CreateNamespace=false
          - RespectIgnoreDifferences=true
          - ApplyOutOfSyncOnly=true
          - ServerSideApply=true
```

Aplicação:
```bash
oc apply -f infra/argocd/apps/applicationset.yaml -n openshift-gitops
```

![](./images/09%20-%20ArgoCD%20Synced%20Applications.png)
![](./images/10%20-%20ArgoCD%20Cluster%20Kafka%20Synced%20Application.png)

---

## 4. O Que Aconteceu "Por Baixo do Capô"?

Durante o primeiro ciclo de sincronização com sucesso, o Argo CD executou o processo de **Resource Adoption**:

1. **Three-Way Merge:** O Argo CD comparou o estado do Git (`Target State`), o estado ao vivo no etcd do OpenShift (`Live State`) e as configurações previamente aplicadas.
2. **Delta Zero na Especificação:** Como os arquivos no repositório Git eram fiéis aos aplicados pelo `oc apply`, o campo `spec` não sofreu alterações.
3. **Injeção do Tracking Identifier:** O Argo CD realizou uma operação cirúrgica de `PATCH` nos objetos no cluster, injetando a anotação:
   ```yaml
   metadata:
     annotations:
       argocd.argoproj.io/tracking-id: kafka-cluster:kafka.strimzi.io/Kafka:kafka-cluster/lab-cluster
   ```
    ![](./images/11%20-%20KafkaNodePool%20CR%20Tracking%20Annotation.png)
    ![](./images/12%20-%20Kafka%20CR%20Tracking%20Annotation.png)
    ![](./images/13%20-%20KafkaTopic%20CR%20Tracking%20Annotation.png)

4. **Preservação de Carga:** Como o Cluster Operator da Strimzi detectou mutações apenas no bloco `metadata.annotations` (e não em variáveis, imagens ou storage da `spec`), ele não agendou nenhum *rolling restart*. Os pods mantiveram o tempo de vida contínuo (`AGE`) e `RESTARTS: 0`.
    ```bash
    imesquit@imesquit-thinkpadt14gen3:~/Customers/Labs/Kafka/GitOps/ocp-kafka-gitops-repo$ oc get pods
    NAME                                           READY   STATUS    RESTARTS   AGE
    lab-cluster-dual-role-0                        1/1     Running   0          68m
    lab-cluster-dual-role-1                        1/1     Running   0          68m
    lab-cluster-dual-role-2                        1/1     Running   0          68m
    lab-cluster-entity-operator-6966bfbfb5-75kvn   2/2     Running   0          68m
    ```
---

## 5. Conclusões e Lições Aprendidas

1. **KRaft no Streams for Apache Kafka 3.2:** A nova geração do operador abandona completamente os pods de ZooKeeper, delegando o controle de quórum e metadados para nós Kafka no modo `controller` através do recurso `KafkaNodePool` na API `v1`.
2. **Importância do Escopo do OperatorGroup:** Para gerenciar topologias corporativas distribuídas em múltiplos namespaces via GitOps, o `OperatorGroup` deve ser configurado como cluster-wide (`spec: {}`).
3. **Estratégia Segura de Adoção Brownfield:** Ao migrar aplicações legadas para o Argo CD, manter `prune: false` e adotar `ServerSideApply=true` garante uma transição transparente sem interrupções operacionais.
4. **RBAC Pré-requisito:** Antes de delegar novos namespaces a pipelines GitOps, é fundamental planejar a matriz de permissões da ServiceAccount do Argo CD para os grupos de APIs envolvidos.
---

## 6. Referências de Consulta

* [Red Hat AMQ Streams / Streams for Apache Kafka Documentation](https://access.redhat.com/documentation/en-us/red_hat_amq_streams/)
* [Red Hat OpenShift GitOps Documentation](https://docs.openshift.com/gitops/)
* [Strimzi Operator Documentation - KRaft and NodePools](https://strimzi.io/docs/operators/latest/overview.html)
* [Argo CD Documentation - Resource Tracking Strategies](https://argo-cd.readthedocs.io/en/stable/user-guide/tracking_strategies/)
* [OpenShift CLI (`oc`) Reference](https://docs.openshift.com/container-platform/latest/cli_reference/openshift_cli/getting-started-cli.html)