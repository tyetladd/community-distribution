# Kubeflow на ноутбуке глазами SRE

**Воркшоп: модульная установка Kubeflow в kind + операторская диагностика через Headlamp**

| | |
|---|---|
| Уровень | Intermediate → High (нужен уверенный kubectl, kustomize, понимание CRD/webhook/RBAC) |
| Длительность | 3 ч 45 мин – 4 ч 15 мин (см. тайминг) |
| Формат | Соло, на своём лэптопе, без облака |
| Версии | Kubeflow Community Distribution `master` (~26.03.x), Kubernetes 1.35/1.36, Headlamp + Kubeflow plugin |
| Дата подготовки | Август 2026 |

---

## 0. Идея и что вы получите

Kubeflow — модульная платформа, где каждая ML-возможность выражена как CRD. Это значит, что вся ML-нагрузка наблюдаема теми же примитивами, что и остальной кластер. На практике же специализированные ML-дашборды (KFP UI, Katib UI, Central Dashboard) прячут слой Kubernetes: когда Trial завис или TrainJob не стартовал, оператор всё равно уходит в `kubectl describe`.

Плагин Headlamp для Kubeflow закрывает этот разрыв — он читает CR напрямую из API-сервера, без промежуточного ML-сервиса и без БД. Воркшоп построен вокруг этой мысли: **вы разворачиваете модульный Kubeflow, ломаете его тремя разными способами и учитесь чинить, глядя на уровень Kubernetes, а не на ML-UI.**

### После воркшопа вы умеете

- Собирать Kubeflow «по кускам» через kustomize вместо full-platform установки — и понимать, что вы отбросили.
- Работать с KFP в **Kubernetes-native режиме**: пайплайны как `Pipeline`/`PipelineVersion` CR, GitOps-совместимо, без зависимости от БД KFP.
- Читать цепочку владения `Experiment → Suggestion → Trial → Job → Pod` (Katib) и `TrainJob → JobSet → Job → Pod` (Trainer v2).
- Диагностировать четыре класса отказов ML-нагрузок: `ImagePullBackOff`, `Unschedulable`, отсутствие метрик из-за неинжектированного sidecar, отказ admission webhook.
- Отвечать на вопрос «что видно в Kubernetes, когда ML-плоскость управления лежит».

### Что мы НЕ ставим (и почему)

Полная платформа Kubeflow по данным upstream требует ≈ **4.4 CPU и 12.3 GiB RAM** только на control plane. Основной вес — Istio (~2.4 GiB), Knative (~1 GiB), KServe, Dashboard, Hub. Для ноутбука это перебор, а для целей воркшопа — балласт.

| Компонент | Ставим? | Что теряем |
|---|---|---|
| cert-manager | ✅ | — (нужен для webhook-сертификатов) |
| Kubeflow Pipelines (k8s-native) | ✅ | — |
| Katib | ✅ (standalone) | Katib UI внутри Central Dashboard |
| Trainer v2 + JobSet | ✅ | — |
| Notebook controller | ✅ (standalone) | Jupyter Web App (создаём `Notebook` через kubectl) |
| Spark Operator | ⭕ бонус | — |
| Istio / oauth2-proxy / Dex | ❌ | Аутентификация, multi-tenancy, mTLS. Доступ к UI — только через `port-forward` |
| Knative + KServe | ❌ | Инференс/serving. Плагин Headlamp их всё равно не покрывает |
| Central Dashboard + Profiles | ❌ | Единый UI, автосоздание namespace по `Profile` |

Итоговый бюджет нашей сборки: **≈ 1.6 CPU и ≈ 4.5 GiB RAM** на control plane + ресурсы под сами задания.

---

## 1. Тайминг

| # | Блок | Время | Активность |
|---|---|---|---|
| 1 | Подготовка хоста | 15 мин | sysctl, бинарники, проверки |
| 2 | Кластер kind | 15 мин | 3 узла, metrics-server |
| 3 | Модульная установка Kubeflow | 50 мин | из них ~30 мин — pull образов (читайте раздел 3.8 параллельно) |
| 4 | Headlamp + плагин | 15 мин | |
| 5 | **Задание 1** — KFP Kubernetes-native | 50 мин | |
| 6 | **Задание 2** — Katib | 45 мин | |
| 7 | **Задание 3** — Trainer v2 / TrainJob | 40 мин | |
| 8 | Бонус — Spark Operator | 20 мин | опционально |
| 9 | Разбор + SRE-runbook | 15 мин | |
| 10 | Очистка | 5 мин | |

> **Совет по темпу.** Блоки 3 и 5–7 упираются в скачивание образов. Запускайте pull заранее (раздел 2.4) и не сидите над прогресс-баром — переходите к чтению следующего раздела.

---

## 2. Предпосылки

### 2.1 Железо и ОС

| Ресурс | Минимум | Комфортно |
|---|---|---|
| RAM | 12 GB | 16+ GB |
| CPU | 4 ядра | 6–8 ядер |
| Свободный диск | 30 GB | 50 GB |
| Сеть | ~8–12 GB трафика на образы | |

- **Linux (amd64)** — эталонная платформа, всё описанное проверено под неё.
- **macOS / Windows+WSL2** — работает, но контейнеры крутятся внутри VM: выдайте Docker Desktop / Colima ≥ 10 GB RAM и ≥ 6 CPU, иначе OOM в самый неинтересный момент.
- **ARM64 (Apple Silicon, aarch64)** — ⚠️ Kubeflow **не полностью поддержан**: часть OCI-образов не собирается под `linux/arm64`. Симптом — `no matching manifest for linux/arm64`. Обходной путь на ноутбуке: запускать через эмуляцию (медленно) или взять x86-хост. Если у вас Apple Silicon и вы не готовы к сюрпризам — арендуйте на 4 часа x86 VM с 16 GB и делайте воркшоп там; всё в этом документе работает по SSH без изменений.
- **GPU не нужен.** Все задания CPU-only.

### 2.2 Что должно быть установлено заранее

- `docker` (или `podman`) — рабочий, ваш пользователь в группе `docker`
- `git`, `curl`, `python3` (3.10+) с `venv`
- `jq` и `yq` — не обязательны, но сильно упрощают жизнь
- Node.js 20+ — **только** если будете собирать плагин Headlamp из исходников (запасной путь, п. 4.3)

### 2.3 Тюнинг ядра (обязательно, Linux)

Кластер поднимет много подов; лимиты inotify по умолчанию упрутся раньше, чем память:

```bash
sudo sysctl fs.inotify.max_user_instances=2280
sudo sysctl fs.inotify.max_user_watches=1255360
```

Чтобы пережило перезагрузку:

```bash
echo -e "fs.inotify.max_user_instances=2280\nfs.inotify.max_user_watches=1255360" \
  | sudo tee /etc/sysctl.d/99-kubeflow-workshop.conf
```

> ⚠️ Симптом упёртого лимита: поды массово в `CrashLoopBackOff` с `too many open files` в логах kubelet, при этом памяти полно. Запомните — на реальных нодах эта же ошибка выглядит как «кластер сошёл с ума».

### 2.4 Лимиты Docker Hub

Часть образов тянется с Docker Hub, у которого жёсткие анонимные лимиты. Залогиньтесь заранее:

```bash
docker login
```

Если поймали `toomanyrequests`, после создания кластера создайте pull-secret:

```bash
kubectl create secret generic regcred \
  --from-file=.dockerconfigjson=$HOME/.docker/config.json \
  --type=kubernetes.io/dockerconfigjson
```

---

## 3. Блок 1: кластер kind (15 мин)

### 3.1 Клонируем манифесты

```bash
mkdir -p ~/kubeflow-workshop && cd ~/kubeflow-workshop
git clone --depth 1 https://github.com/kubeflow/manifests.git
cd manifests
```

> **Про версии.** Мы работаем с `master`: релизы теперь календарные (`26.03`, `26.03.1`, следующий — `26.10`), и пути к оверлеям между релизами двигаются. Все пути ниже проверены на `master` в августе 2026. Если что-то не собирается — **сначала посмотрите, что реально лежит в дереве** (`git ls-tree -d --name-only HEAD applications/...`), а не гуглите ошибку. Для консервативного варианта: `git tag -l` и `git checkout <tag>`, но тогда сверяйте пути с README именно того тега.

### 3.2 Поднимаем кластер

В репозитории есть CI-скрипт, который ставит согласованные версии `kind`, `kubectl`, `kustomize` (на момент подготовки — kind v0.32.0, node image Kubernetes v1.36.1, kustomize v5.8.1) и создаёт кластер `kubeflow` из 1 control-plane + 2 worker:

```bash
./tests/install_KinD_create_KinD_cluster_install_kustomize.sh
export PATH="$HOME/.local/bin:$PATH"
```

Скрипт настраивает `service-account-issuer` и `service-account-signing-key-file` — это нужно для projected volumes с SA-токенами, на которые опираются KFP и Trainer. Если будете городить свой `kind` config — не потеряйте эти два параметра.

Зафиксируем kubeconfig, чтобы случайно не наложить Kubeflow на рабочий кластер:

```bash
kind get kubeconfig --name kubeflow > /tmp/kubeflow-config
export KUBECONFIG=/tmp/kubeflow-config
kubectl config current-context   # ожидаем kind-kubeflow
kubectl get nodes -o wide
```

> 🔒 **Правило воркшопа:** каждый новый терминал начинается с `export KUBECONFIG=/tmp/kubeflow-config && export PATH="$HOME/.local/bin:$PATH"`.

> ⚠️ **Известная проблема: Docker Engine 27+ ломает межузловой pod-трафик в multi-node `kind`.** Начиная с Docker 27 дефолтная политика хостовой цепочки `iptables FORWARD` — `DROP`, а разрешающие правила самого Docker покрывают только трафик к «родным» IP контейнеров в bridge-сети, но не транзитную маршрутизацию pod-подсетей *через* контейнер-узел — а именно на этом держится связность между узлами в `kind`. Не специфично для Kubeflow, но 3-нодовый кластер этого воркшопа ловит проблему гарантированно. Симптом: под/сервис на одном узле недоступен с другого узла (зависает до таймаута, а не `connection refused`), при этом трафик в пределах одного узла и между самими Docker-контейнерами узлов работает нормально — из-за этого баг легко спутать с чем-то другим (например, с NetworkPolicy или webhook-таймаутом из п. 4.2).
>
> Проверка:
> ```bash
> BRIDGE="br-$(docker network inspect kind -f '{{.Id}}' | cut -c1-12)"
> sudo iptables -C DOCKER-USER -i "$BRIDGE" -o "$BRIDGE" -j ACCEPT
> # "No rule found" ⇒ межузловой трафик блокируется хостом
> ```
> Фикс — одно правило в `DOCKER-USER` (эту цепочку Docker специально держит пустой для пользовательских исключений и не трогает её при своих апдейтах):
> ```bash
> sudo iptables -I DOCKER-USER -i "$BRIDGE" -o "$BRIDGE" -j ACCEPT
> ```
> Правило живёт только до рестарта Docker/WSL2. Чтобы не повторять руками при каждой перезагрузке — systemd-юнит, который сам находит бридж сети `kind` и накатывает правило после старта `docker.service` (идемпотентно, безопасно перезапускать):
> ```bash
> sudo install -m 755 workshop/scripts/kind-docker-user-fix.sh /usr/local/sbin/kind-docker-user-fix.sh
> sudo install -m 644 workshop/scripts/kind-docker-user-fix.service /etc/systemd/system/kind-docker-user-fix.service
> sudo systemctl daemon-reload
> sudo systemctl enable --now kind-docker-user-fix.service
> ```

### 3.3 metrics-server

Нужен, чтобы `kubectl top` и панели ресурсов в Headlamp показывали реальные цифры (в kind требуется `--kubelet-insecure-tls`, скрипт это делает):

```bash
./tests/metrics-server_install.sh
kubectl top nodes    # может отдать данные не сразу, ~60 сек
```

### 3.4 Прогреваем кэш образов (запустите в фоне)

Три ноды kind — три независимых image cache. Предзагрузка сильно ускорит задания:

```bash
for img in \
  ghcr.io/kubeflow/katib/pytorch-mnist-cpu:v0.19.0 \
  python:3.11-slim \
  busybox:1.36 ; do
  docker pull "$img" && kind load docker-image "$img" --name kubeflow &
done
```

`kind load docker-image` кладёт образ сразу на все ноды — заодно хороший приём для air-gapped кластеров.

### ✅ Чекпоинт 1

```bash
kubectl get nodes            # 3 узла Ready
kubectl top nodes            # цифры есть
kubectl get --raw /healthz   # ok
```

---

## 4. Блок 2: модульная установка Kubeflow (50 мин)

Все команды — из корня `~/kubeflow-workshop/manifests`.

> **Важно про `kubectl apply` и CRD.** Kustomize отдаёт CRD и CR одним потоком, а CRD не успевает стать `Established`. Ошибка `no matches for kind "..."` на первой попытке — **нормально**. Решение — повторить команду. Именно поэтому в upstream-документации везде фигурируют retry-циклы, а не «apply один раз».

### 4.1 Namespace и cert-manager

```bash
kustomize build common/kubeflow-namespace/base | kubectl apply -f -
./tests/cert_manager_install.sh
```

Скрипт ставит base, дожидается готовности webhook, затем накатывает оверлей `kubeflow` (self-signed ClusterIssuer). cert-manager обслуживает admission-webhook сертификаты для KFP, Trainer, Spark — без него дальше ничего не поедет.

Проверка:

```bash
kubectl -n cert-manager get pods
kubectl get clusterissuers
```

### 4.2 Kubeflow Pipelines в Kubernetes-native режиме

Ключевой выбор воркшопа. У KFP два режима хранения определений пайплайнов:

- **классический** — пайплайны лежат в MySQL, `kubectl` о них ничего не знает;
- **Kubernetes-native** — пайплайны становятся CR `Pipeline` и `PipelineVersion` (`pipelines.kubeflow.org/v2beta1`), валидация идёт через admission webhook, REST API просто транслируется в Kubernetes API.

Второй режим — то, ради чего плагин Headlamp вообще может показать состояние пайплайнов без обращения к БД. Ставим single-user вариант (без multi-tenancy, потому что Istio у нас нет).

**Шаг 1 — cluster-scoped ресурсы (обязательно, до основного оверлея!).** Оверлеи KFP намеренно разделены: namespace-scoped часть отдельно, CRD и cluster-scoped RBAC отдельно. Сюда входят **CRD Argo Workflows**, а также `scheduledworkflows` и `viewers`:

```bash
kustomize build applications/pipeline/upstream/env/cert-manager/cluster-scoped-resources \
  | kubectl apply --server-side --force-conflicts -f -

kubectl get crd | grep -E 'argoproj|kubeflow.org'
```

> ⚠️ Если пропустить этот шаг, установка внешне пройдёт (apply не ругнётся — Workflow-объекты в оверлее не создаются), но `workflow-controller` уйдёт в `CrashLoopBackOff` на старте, не сумев завести informer для `workflows.argoproj.io`, а `ml-pipeline-viewer-crd` начнёт рестартовать. Ни один Run при этом не запустится. Это одна из самых частых ошибок при ручной сборке KFP.

**Шаг 2 — основной оверлей:**

```bash
while ! kustomize build applications/pipeline/upstream/env/cert-manager/platform-agnostic-k8s-native \
  | kubectl apply --server-side --force-conflicts -f - ; do
  echo "retrying..."; sleep 20
done
```

Ждём (это самый долгий шаг, 10–20 мин на скачивание):

```bash
kubectl -n kubeflow rollout status deploy/ml-pipeline --timeout=900s
kubectl -n kubeflow rollout status deploy/ml-pipeline-ui --timeout=600s
kubectl -n kubeflow get pods
```

Состав, который стоит опознать глазами (пригодится в Задании 1):

| Под | Роль |
|---|---|
| `ml-pipeline` | API-сервер KFP |
| `ml-pipeline-ui` | веб-интерфейс |
| `ml-pipeline-persistenceagent` | синхронизация статусов Argo Workflow → KFP |
| `ml-pipeline-scheduledworkflow` | контроллер `RecurringRun` |
| `workflow-controller` | Argo Workflows — реальный исполнитель шагов |
| `metadata-grpc-deployment` | ML Metadata (трекинг артефактов) |
| `mysql` | БД KFP (в k8s-native режиме — только runs/metadata, не определения) |
| `seaweedfs` | S3-совместимое хранилище артефактов (заменило minio) |

> **Известная шероховатость: `cache-server` в `ContainerCreating`.** Под монтирует секрет `webhook-server-tls`, который в этом оверлее должен создавать `cache-deployer`. Если `cache-deployer` не поднялся (проверьте `kubectl -n kubeflow get deploy | grep cache`), под будет вечно висеть с `FailedMount`. Два выхода: выдать сертификат через cert-manager вручную (как это делает multi-user оверлей) —
>
> ```bash
> kubectl apply -f - <<'EOF'
> apiVersion: cert-manager.io/v1
> kind: Issuer
> metadata: {name: kfp-cache-selfsigned-issuer, namespace: kubeflow}
> spec: {selfSigned: {}}
> ---
> apiVersion: cert-manager.io/v1
> kind: Certificate
> metadata: {name: kfp-cache-cert, namespace: kubeflow}
> spec:
>   commonName: kfp-cache-cert
>   isCA: true
>   dnsNames: [cache-server, cache-server.kubeflow, cache-server.kubeflow.svc]
>   issuerRef: {kind: Issuer, name: kfp-cache-selfsigned-issuer}
>   secretName: webhook-server-tls
> EOF
> kubectl -n kubeflow delete pod -l app=cache-server
> ```
>
> — либо просто отключить кэш шагов: `kubectl -n kubeflow scale deploy/cache-server --replicas=0`. На задания воркшопа он не влияет.

> ⚠️ **Известный баг апстрима: `NetworkPolicy ml-pipeline` не пускает kube-apiserver к admission-вебхуку `PipelineVersion`.** `ml-pipeline` в k8s-native режиме сам обслуживает `pipelineversions.pipelines.kubeflow.org` mutating/validating webhook на порту 8443. Вызывает его напрямую kube-apiserver — а он не под и не матчится через `podSelector`/`namespaceSelector`, под которые заточена `NetworkPolicy ml-pipeline` (`common/kubeflow-namespace/base/kubeflow/ml-pipeline.yaml`). Симптом: `Pipeline` создаётся (на него хука нет), а `kubectl apply` любой `PipelineVersion` виснет и падает по таймауту:
> ```
> failed calling webhook "pipelineversions.pipelines.kubeflow.org": failed to call webhook:
> Post "https://ml-pipeline.kubeflow.svc:8443/webhooks/mutate-pipelineversion?timeout=30s":
> net/http: request canceled while waiting for connection (Client.Timeout exceeded while awaiting headers)
> ```
> У всех остальных вебхук-сервисов в этих манифестах (`katib-controller`, `spark-operator-webhook`, `jobset-webhook`, `trainer-webhook`) `NetworkPolicy` на webhook-порт состоит только из `ports`, без `from` — это осознанный паттерн (защита вебхука — его собственные TLS/CA, а не сетевая изоляция), просто `ml-pipeline` под него не подпадает. Заведено вверх по течению: [issue #3580](https://github.com/kubeflow/community-distribution/issues/3580), [PR #3581](https://github.com/kubeflow/community-distribution/pull/3581).
>
> Пока PR не смёржен — живой патч на своём кластере:
> ```bash
> kubectl -n kubeflow patch networkpolicy ml-pipeline --type='json' -p='[
>   {"op":"add","path":"/spec/ingress/-","value":{"ports":[{"protocol":"TCP","port":8443}]}}
> ]'
> ```

Проверяем, что API-группа приехала:

```bash
kubectl get crd | grep pipelines.kubeflow.org
kubectl api-resources --api-group=pipelines.kubeflow.org
```

### 4.3 Katib (standalone)

`katib-with-kubeflow` тянет за собой интеграцию с Central Dashboard и Istio. Нам нужен `katib-standalone`:

```bash
cd applications/katib/upstream
kustomize build installs/katib-standalone | kubectl apply --server-side --force-conflicts -f -
cd -

kubectl wait --for=condition=Available deploy/katib-controller  -n kubeflow --timeout=300s
kubectl wait --for=condition=Available deploy/katib-db-manager  -n kubeflow --timeout=300s
kubectl wait --for=condition=Available deploy/katib-mysql       -n kubeflow --timeout=300s
```

### 4.4 Trainer v2 (+ JobSet)

Обратите внимание на порядок: сначала CRD, дожидаемся `Established`, потом контроллеры, потом runtimes. Это классический порядок для любого CRD-heavy оператора.

```bash
cd applications/trainer

kustomize build upstream/base/crds | kubectl apply --server-side --force-conflicts -f -
sleep 5
kubectl wait --for condition=established crd/trainjobs.trainer.kubeflow.org --timeout=60s

# ВНИМАНИЕ: overlays, а НЕ upstream/overlays.
# applications/trainer/overlays — обёртка репозитория манифестов, она задаёт
# namespace: kubeflow-system поверх upstream/overlays/kubeflow-platform.
# В upstream/overlays лежат только под-оверлеи и своего kustomization.yaml там нет.
kustomize build overlays | kubectl apply --server-side --force-conflicts -f -
kubectl wait --for=condition=Available deploy/kubeflow-trainer-controller-manager -n kubeflow-system --timeout=600s
kubectl wait --for=condition=Available deploy/jobset-controller-manager           -n kubeflow-system --timeout=300s

kustomize build upstream/overlays/runtimes | kubectl apply --server-side --force-conflicts -f -
cd -

kubectl get clustertrainingruntimes
```

Дожидаемся, пока cert-manager проставит CA-бандлы в webhook-конфигурации (без этого TrainJob будет отвергаться с TLS-ошибкой):

```bash
kubectl wait --timeout=180s \
  --for='jsonpath={.webhooks[0].clientConfig.caBundle}' \
  validatingwebhookconfiguration/validator.trainer.kubeflow.org
kubectl wait --timeout=180s \
  --for='jsonpath={.webhooks[0].clientConfig.caBundle}' \
  mutatingwebhookconfiguration/jobset-mutating-webhook-configuration
```

> Заметьте: Trainer живёт в `kubeflow-system`, а KFP и Katib — в `kubeflow`. Проект в процессе миграции namespace. Это первое, обо что спотыкаются при отладке.

### 4.5 Notebook controller (standalone)

Полный стек Notebooks тянет Jupyter Web App + Istio VirtualService. Нам достаточно контроллера — `Notebook` CR будем создавать через kubectl, а разглядывать через Headlamp:

```bash
kustomize build applications/notebooks-v1/upstream/notebook-controller/overlays/standalone \
  | kubectl apply --server-side --force-conflicts -f -
kubectl get crd notebooks.kubeflow.org
```

Если оверлей `standalone` в вашей ревизии отсутствует — поставьте только CRD (плагин Headlamp активирует раздел уже по факту наличия API-группы):

```bash
kustomize build applications/notebooks-v1/upstream/notebook-controller/crd | kubectl apply -f -
```

### 4.6 Spark Operator (бонус)

```bash
kustomize build applications/spark/spark-operator/overlays/kubeflow \
  | kubectl -n kubeflow apply --server-side --force-conflicts -f -
kubectl -n kubeflow wait --for=condition=Available deploy/spark-operator-controller --timeout=300s
kubectl -n kubeflow wait --for=condition=Available deploy/spark-operator-webhook     --timeout=300s
```

### 4.7 Рабочий namespace

```bash
kubectl create namespace ml-workshop
kubectl label namespace ml-workshop katib.kubeflow.org/metrics-collector-injection=enabled
```

Метка критична: именно по ней mutating webhook Katib инжектит sidecar-сборщик метрик в поды Trial. **Запомните этот факт — в Задании 2 мы его сломаем намеренно.**

### 4.8 Итоговая проверка API-поверхности

Плагин Headlamp включает разделы по факту обнаружения API-групп. Проверим, что обнаруживать есть что:

```bash
for g in kubeflow.org pipelines.kubeflow.org trainer.kubeflow.org sparkoperator.k8s.io; do
  echo "=== $g"; kubectl api-resources --api-group=$g 2>/dev/null
done
```

Ожидаем увидеть:

| Группа | Ресурсы | Компонент |
|---|---|---|
| `kubeflow.org/v1` | `notebooks` | Notebooks |
| `kubeflow.org/v1beta1` | `experiments`, `trials`, `suggestions` | Katib |
| `pipelines.kubeflow.org/v2beta1` | `pipelines`, `pipelineversions` | KFP |
| `trainer.kubeflow.org/v1alpha1` | `trainjobs`, `trainingruntimes`, `clustertrainingruntimes` | Trainer |
| `sparkoperator.k8s.io/v1beta2` | `sparkapplications`, `scheduledsparkapplications` | Spark (бонус) |

### ✅ Чекпоинт 2

```bash
kubectl get pods -A | grep -vE 'Running|Completed'   # пусто (или только Job'ы)
kubectl top nodes
kubectl get crd | grep -cE 'kubeflow|jobset|sparkoperator'
```

Если что-то в `Pending` — смотрите раздел «Приложение A».

---

## 5. Блок 3: Headlamp и плагин Kubeflow (15 мин)

### 5.1 Ставим Headlamp

Headlamp — расширяемый веб-UI для Kubernetes под эгидой Kubernetes SIG UI, Apache 2.0. Работает как десктоп-приложение или in-cluster. Для воркшопа берём десктоп: [headlamp.dev](https://headlamp.dev) → загрузка под вашу ОС.

> macOS/Windows покажут предупреждение о неподписанном приложении — это ожидаемо, см. документацию Headlamp.

Запустите Headlamp. Он подхватит kubeconfig; если у вас kubeconfig в `/tmp/kubeflow-config`, добавьте кластер вручную через UI либо запустите приложение с `KUBECONFIG=/tmp/kubeflow-config`.

### 5.2 Ставим плагин через Plugin Catalog

1. В сайдбаре — **Plugin Catalog**.
2. Поиск: `Kubeflow`.
3. **Install**, затем перезапустите Headlamp.

Если плагина не видно — снимите в настройках каталога галку «Only official plugins».

### 5.3 Запасной путь: сборка из исходников

```bash
cd ~/kubeflow-workshop
git clone --depth 1 https://github.com/headlamp-k8s/plugins.git headlamp-plugins
cd headlamp-plugins/kubeflow
npm install
npm run build

# Linux
mkdir -p ~/.config/Headlamp/plugins/kubeflow
cp -r dist package.json ~/.config/Headlamp/plugins/kubeflow/
# macOS: ~/Library/Application Support/Headlamp/plugins/kubeflow
```

Перезапустите Headlamp. Для итеративной разработки вместо `build` используйте `npm run start` — watcher компилирует прямо в папку плагинов и включает hot-reload.

### 5.4 Проверка

В сайдбаре должны появиться разделы Kubeflow: Notebooks, Pipelines, Katib, Training, (Spark — если ставили). Разделов ровно столько, сколько API-групп нашлось в кластере — это и есть заявленное авто-обнаружение.

**Мини-упражнение (2 мин).** Удалите CRD Notebook и посмотрите, исчезнет ли раздел:

```bash
kubectl delete crd notebooks.kubeflow.org
# обновите страницу Headlamp → раздел Notebooks должен пропасть
kustomize build applications/notebooks-v1/upstream/notebook-controller/crd | kubectl apply -f -
```

### ✅ Чекпоинт 3

Headlamp подключён к `kind-kubeflow`, разделы Kubeflow видны, `Map` в сайдбаре открывается.

---

## 6. Задание 1 — Kubeflow Pipelines в Kubernetes-native режиме (50 мин)

### Цель

Понять, где физически живёт определение пайплайна, научиться версионировать пайплайны как обычные Kubernetes-ресурсы и увидеть разницу между тем, что показывает KFP UI, и тем, что показывает кластер.

### 6.1 Готовим SDK

```bash
cd ~/kubeflow-workshop
python3 -m venv .venv && source .venv/bin/activate
pip install --upgrade "kfp>=2.16"
python -c "import kfp; print(kfp.__version__)"
```

### 6.2 Пишем пайплайн

`pipeline_v1.py`:

```python
from kfp import dsl, compiler
from kfp.compiler.compiler_utils import KubernetesManifestOptions

NAMESPACE = "kubeflow"

@dsl.component(base_image="python:3.11-slim")
def make_dataset(rows: int) -> int:
    import random
    data = [random.random() for _ in range(rows)]
    print(f"generated {len(data)} rows, mean={sum(data)/len(data):.4f}")
    return len(data)

@dsl.component(base_image="python:3.11-slim")
def train_stub(n_rows: int, epochs: int) -> float:
    import time
    loss = 1.0
    for e in range(epochs):
        loss *= 0.7
        print(f"epoch={e} rows={n_rows} loss={loss:.4f}")
        time.sleep(2)
    return loss

@dsl.pipeline(name="workshop-pipeline", description="SRE workshop demo pipeline")
def workshop_pipeline(rows: int = 1000, epochs: int = 3) -> float:
    ds = make_dataset(rows=rows)
    tr = train_stub(n_rows=ds.output, epochs=epochs)
    return tr.output

compiler.Compiler().compile(
    pipeline_func=workshop_pipeline,
    package_path="workshop-pipeline-v1.yaml",
    kubernetes_manifest_format=True,
    kubernetes_manifest_options=KubernetesManifestOptions(
        pipeline_name="workshop-pipeline",
        pipeline_display_name="Workshop Pipeline",
        pipeline_version_name="workshop-pipeline-v1",
        pipeline_version_display_name="v1 — baseline",
        namespace=NAMESPACE,
        include_pipeline_manifest=True,
    ),
)
```

```bash
python pipeline_v1.py
head -30 workshop-pipeline-v1.yaml
```

**Остановитесь и посмотрите в файл.** Вы получили не «артефакт для загрузки в UI», а два обычных Kubernetes-манифеста: `Pipeline` (метаданные) и `PipelineVersion` (в `spec.pipelineSpec` лежит скомпилированный IR — сериализованный protobuf `PipelineSpec`). Это и есть GitOps-совместимое представление пайплайна.

### 6.3 Применяем и проверяем

```bash
kubectl apply -f workshop-pipeline-v1.yaml
kubectl -n kubeflow get pipelines,pipelineversions
kubectl -n kubeflow get pipelineversion workshop-pipeline-v1 -o yaml | head -40
```

### 6.4 Запускаем Run

```bash
kubectl port-forward -n kubeflow svc/ml-pipeline-ui 8080:80
```

Откройте `http://localhost:8080`. Пайплайн уже там — его никто не «загружал», API-сервер просто прочитал CR. Запустите Run с параметрами по умолчанию.

Параллельно в другом терминале смотрите, что происходит на уровне Kubernetes:

```bash
kubectl -n kubeflow get workflows.argoproj.io -w
kubectl -n kubeflow get pods -l workflows.argoproj.io/workflow --watch
```

**В Headlamp:** Pipelines → Runs (состояние и длительность), Pipelines → Artifacts (агрегированные `pipelineRoot` из недавних Run).

### 6.5 Версионирование и diff

Скопируйте `pipeline_v1.py` в `pipeline_v2.py` и внесите содержательное изменение — например, `epochs: int = 8` и коэффициент `0.5` вместо `0.7`. Поменяйте:

```python
pipeline_version_name="workshop-pipeline-v2",
pipeline_version_display_name="v2 — longer training",
```

и `include_pipeline_manifest=False` (объект `Pipeline` уже существует).

```bash
python pipeline_v2.py
kubectl apply -f workshop-pipeline-v2.yaml
kubectl -n kubeflow get pipelineversions
```

**В Headlamp:** откройте детали Pipeline `workshop-pipeline` — плагин показывает **параллельный YAML-diff последней и предыдущей PipelineVersion**. Найдите в диффе именно ваши правки. Это ровно то, чего нет в KFP UI, и ровно то, что нужно на code review инфраструктурного изменения.

### 6.6 🔥 Инъекция сбоя A: битый образ

`pipeline_v3.py` — то же самое, но у компонента:

```python
@dsl.component(base_image="ghcr.io/kubeflow/definitely-not-a-real-image:v0")
def train_stub(n_rows: int, epochs: int) -> float:
    ...
```

версия `workshop-pipeline-v3`, применить, запустить Run из UI.

**Задача:** пользуясь **только Headlamp**, ответьте:
1. На каком шаге пайплайна встала работа?
2. Какая именно `reason`/`message` у Pod condition?
3. Сколько раз kubelet пытался и с каким backoff?
4. Что об этом же сказал KFP UI — и достаточно ли этого, чтобы завести тикет команде ML?

Контрольная проверка через kubectl:

```bash
kubectl -n kubeflow get pods --field-selector=status.phase=Pending
kubectl -n kubeflow describe pod <pod> | sed -n '/Events/,$p'
```

### 6.7 🔥 Инъекция сбоя B: ML-плоскость управления лежит

Главный тезис статьи — представления Pipelines читают Kubernetes API напрямую и не ходят в API-сервис KFP и его БД. Проверим:

```bash
kubectl -n kubeflow scale deploy/ml-pipeline --replicas=0
kubectl -n kubeflow scale deploy/ml-pipeline-ui --replicas=0
```

Обновите Headlamp. **Вопрос:** видны ли ваши `Pipeline`/`PipelineVersion`? Виден ли diff версий? А что показывает `http://localhost:8080`?

Верните обратно:

```bash
kubectl -n kubeflow scale deploy/ml-pipeline --replicas=1
kubectl -n kubeflow scale deploy/ml-pipeline-ui --replicas=1
```

### 6.8 Вопросы для самопроверки

1. В классическом режиме KFP определение пайплайна лежит в MySQL. Что при этом ломается в сценарии «восстановление кластера из манифестов в Git»?
2. Кто именно валидирует `PipelineVersion` при `kubectl apply` — контроллер или admission webhook? Проверьте: `kubectl get validatingwebhookconfigurations | grep -i pipeline`.
3. Попробуйте применить `PipelineVersion` с намеренно битым `pipelineSpec` (удалите обязательное поле). На каком этапе прилетит отказ и как выглядит текст?
4. Argo Workflow создаёт поды шагов. Кто владелец пода — Workflow или KFP Run? Проверьте `.metadata.ownerReferences`. Как это отражается на графе в Headlamp Map?

---

## 7. Задание 2 — Katib: подбор гиперпараметров и молчащие метрики (45 мин)

### Цель

Разобрать цепочку `Experiment → Suggestion → Trial → Job → Pod`, увидеть, откуда берётся «лучший Trial», и научиться диагностировать самый коварный отказ Katib — когда Trial успешно завершается, но метрик нет.

### 7.1 Эксперимент

`experiment.yaml`:

```yaml
apiVersion: kubeflow.org/v1beta1
kind: Experiment
metadata:
  name: mnist-random
  namespace: ml-workshop
spec:
  objective:
    type: minimize
    goal: 0.05
    objectiveMetricName: loss
  algorithm:
    algorithmName: random
  earlyStopping:
    algorithmName: medianstop
    algorithmSettings:
      - name: min_trials_required
        value: "2"
  parallelTrialCount: 2
  maxTrialCount: 6
  maxFailedTrialCount: 2
  parameters:
    - name: lr
      parameterType: double
      feasibleSpace: {min: "0.01", max: "0.05", step: "0.005"}
    - name: momentum
      parameterType: double
      feasibleSpace: {min: "0.5", max: "0.9", step: "0.1"}
  trialTemplate:
    primaryContainerName: training-container
    trialParameters:
      - name: learningRate
        description: Learning rate
        reference: lr
      - name: momentum
        description: Momentum
        reference: momentum
    trialSpec:
      apiVersion: batch/v1
      kind: Job
      spec:
        template:
          spec:
            containers:
              - name: training-container
                image: ghcr.io/kubeflow/katib/pytorch-mnist-cpu:v0.19.0
                command:
                  - "python3"
                  - "/opt/pytorch-mnist/mnist.py"
                  - "--epochs=1"
                  - "--batch-size=64"
                  - "--lr=${trialParameters.learningRate}"
                  - "--momentum=${trialParameters.momentum}"
                resources:
                  requests: {cpu: "500m", memory: "1Gi"}
                  limits:   {cpu: "1",    memory: "2Gi"}
            restartPolicy: Never
```

```bash
kubectl apply -f experiment.yaml
kubectl -n ml-workshop get experiment,suggestion,trial -w
```

### 7.2 Читаем цепочку

```bash
# Suggestion — это отдельный под с алгоритмом подбора
kubectl -n ml-workshop get suggestion mnist-random -o yaml | yq '.status'

# Кто чей владелец
kubectl -n ml-workshop get trial -o custom-columns=\
'NAME:.metadata.name,OWNER:.metadata.ownerReferences[0].kind,STATUS:.status.conditions[-1].type'

kubectl -n ml-workshop get pods -o custom-columns=\
'NAME:.metadata.name,OWNER:.metadata.ownerReferences[0].kind,CONTAINERS:.spec.containers[*].name'
```

Обратите внимание: в поде Trial **два** контейнера — `training-container` и `metrics-logger-and-collector`. Второй инжектирован mutating webhook'ом Katib по метке namespace.

**В Headlamp:** Katib → Experiments → `mnist-random`. Плагин показывает алгоритм подбора, пространство поиска, каждый Trial со статусом, текущий лучший Trial с метриками и назначениями параметров, а также настройку early stopping и число досрочно остановленных Trial.

Сравните с источником истины:

```bash
kubectl -n ml-workshop get experiment mnist-random \
  -o jsonpath='{.status.currentOptimalTrial}' | yq -P
```

### 7.3 🔥 Инъекция сбоя A: неправильное имя метрики

Пересоздайте эксперимент, поменяв цель на несуществующую метрику:

```bash
kubectl -n ml-workshop delete experiment mnist-random
sed 's/objectiveMetricName: loss/objectiveMetricName: val_loss/; s/name: mnist-random/name: mnist-badmetric/' \
  experiment.yaml | kubectl apply -f -
```

Наблюдайте: Job'ы отрабатывают успешно, поды в `Completed`, а Trial'ы не получают результат, эксперимент не сходится.

**Задача:** локализуйте причину за 5 минут. Подсказки, куда смотреть:

```bash
kubectl -n ml-workshop logs <trial-pod> -c metrics-logger-and-collector | tail -30
kubectl -n ml-workshop get trial <trial> -o jsonpath='{.status.observation}'
```

**Вопрос:** это отказ инфраструктуры или ошибка пользователя? Как бы вы отличили одно от другого **автоматически** — какой сигнал/алерт вы бы повесили как SRE платформы?

### 7.4 🔥 Инъекция сбоя B: sidecar не инжектится

```bash
kubectl label namespace ml-workshop katib.kubeflow.org/metrics-collector-injection-
kubectl -n ml-workshop delete experiment mnist-badmetric
kubectl apply -f experiment.yaml
```

**Задача:** посмотрите на поды новых Trial через Headlamp. Сколько контейнеров? Что при этом видно в `Experiment.status`? Сформулируйте одним предложением разницу симптомов между сбоем A и сбоем B — по внешнему виду они почти одинаковы, а причины и владельцы проблемы разные.

Вернуть:

```bash
kubectl label namespace ml-workshop katib.kubeflow.org/metrics-collector-injection=enabled --overwrite
```

### 7.5 🔥 Инъекция сбоя C: не влезли в ноду

Поднимите `parallelTrialCount` до 6 и `requests.cpu` до `3`:

```bash
kubectl -n ml-workshop patch experiment mnist-random --type=merge \
  -p '{"spec":{"parallelTrialCount":6}}'
```

(и отредактируйте requests в `experiment.yaml`, переприменив эксперимент под новым именем).

**Задача:** найдите в Headlamp поды в `Pending`, прочитайте Pod conditions и `reason`. Ответьте: у кого «болит» — у Katib, у планировщика или у автора эксперимента? Что показывает `kubectl -n ml-workshop describe pod <pod> | grep -A5 Events`?

### 7.6 Вопросы для самопроверки

1. Почему удаление `Experiment` каскадно убивает Trial'ы, а удаление Trial не убивает Experiment? Найдите `ownerReferences` и `blockOwnerDeletion`.
2. Что произойдёт с идущим экспериментом, если убить под `Suggestion`? Проверьте: `kubectl -n ml-workshop delete pod -l suggestion-name=mnist-random`.
3. Katib пишет результаты в `katib-mysql`. Что покажет Headlamp, если отскейлить `katib-db-manager` в 0? А `katib-controller`? Объясните разницу.
4. Early stopping: сколько Trial было остановлено досрочно и по какому критерию? Где это видно в CR, а где — в UI?

---

## 8. Задание 3 — Trainer v2: TrainJob и распределённая топология (40 мин)

### Цель

Понять новую модель Trainer v2 (`TrainJob` + `TrainingRuntime` вместо PyTorchJob/TFJob), увидеть развёртывание в JobSet и научиться отличать «оператор не принял манифест» от «планировщик не смог разместить».

### 8.1 Смотрим runtimes

```bash
kubectl get clustertrainingruntimes
kubectl get clustertrainingruntime torch-distributed -o yaml | yq '.spec'
```

Разберите: `mlPolicy.numNodes`, `torch: {}`, `template.spec.replicatedJobs` — это шаблон JobSet, поверх которого TrainJob накладывает свои параметры.

> ⚠️ Дефолтный образ в `torch-distributed` — CUDA-сборка PyTorch (несколько ГБ, GPU-ориентированная). Для ноутбука мы переопределим образ в TrainJob.

### 8.2 TrainJob (лёгкий вариант)

`trainjob.yaml`:

```yaml
apiVersion: trainer.kubeflow.org/v1alpha1
kind: TrainJob
metadata:
  name: topology-probe
  namespace: ml-workshop
spec:
  runtimeRef:
    apiGroup: trainer.kubeflow.org
    kind: ClusterTrainingRuntime
    name: torch-distributed
  trainer:
    image: python:3.11-slim
    numNodes: 2
    numProcPerNode: 1
    command: ["python3", "-c"]
    args:
      - |
        import os, time, socket
        keys = sorted(k for k in os.environ if k.startswith(("PET_", "MASTER_", "RANK", "WORLD", "NODE")))
        print("host:", socket.gethostname())
        for k in keys:
            print(f"{k}={os.environ[k]}")
        time.sleep(90)
        print("done")
    resourcesPerNode:
      requests: {cpu: "200m", memory: "256Mi"}
      limits:   {cpu: "500m", memory: "512Mi"}
```

```bash
kubectl apply -f trainjob.yaml
kubectl -n ml-workshop get trainjob,jobset,jobs,pods
```

Посмотрите логи любого пода — вы увидите переменные окружения `torchrun`, которые оператор проставил сам (`PET_NNODES`, `PET_NPROC_PER_NODE`, `MASTER_ADDR`, `RANK`, ...). Именно это и есть «работа» runtime: пользователь описал *что* запускать, оператор описал *как* это связать в распределённый запуск.

```bash
kubectl -n ml-workshop logs -l jobset.sigs.k8s.io/jobset-name=topology-probe --tail=40 --prefix
```

### 8.3 Цепочка владения

```bash
kubectl -n ml-workshop get pods -o custom-columns=\
'POD:.metadata.name,OWNER_KIND:.metadata.ownerReferences[0].kind,OWNER:.metadata.ownerReferences[0].name'

kubectl -n ml-workshop get jobs -o custom-columns=\
'JOB:.metadata.name,OWNER_KIND:.metadata.ownerReferences[0].kind,OWNER:.metadata.ownerReferences[0].name'
```

Должно получиться: `TrainJob → JobSet → Job → Pod`.

**В Headlamp:** Training → TrainJobs → `topology-probe`. Проверьте, что TrainJob ссылается на ожидаемый runtime (одна из целевых операторских задач, заявленных плагином). Затем откройте **Map** — плагин рендерит Notebook, Profile, PodDefault, Experiment, Pipeline, SparkApplication и TrainJob как узлы графа и рисует рёбра по `.metadata.ownerReferences`.

### 8.4 🔥 Инъекция сбоя A: несуществующий runtime

```bash
kubectl -n ml-workshop create -f - <<'EOF'
apiVersion: trainer.kubeflow.org/v1alpha1
kind: TrainJob
metadata:
  name: broken-runtime
  namespace: ml-workshop
spec:
  runtimeRef:
    name: torch-distributed-typo
  trainer:
    image: python:3.11-slim
    numNodes: 1
    command: ["true"]
EOF
```

**Вопросы:**
1. Отказ пришёл сразу от `kubectl` или объект создался и «завис»? Кто отказал — validating webhook или контроллер?
2. Если объект всё же создался — что в `.status.conditions`?
3. Заметьте: `runtimeRef` помечен в CRD как **immutable** (`self == oldSelf`). Попробуйте исправить опечатку через `kubectl edit`. Что произойдёт и почему это правильное проектное решение?

### 8.5 🔥 Инъекция сбоя B: не помещается

```bash
kubectl -n ml-workshop delete trainjob topology-probe
sed 's/name: topology-probe/name: too-big/; s/cpu: "200m"/cpu: "6"/' trainjob.yaml | kubectl apply -f -
kubectl -n ml-workshop get pods
```

**Задача:** через Headlamp дойдите от TrainJob до конкретного Pod condition. Ответьте, в чём принципиальная разница сообщений и «кому нести тикет» между сбоем A (webhook/контроллер) и сбоем B (планировщик).

### 8.6 Опционально: настоящее обучение

Если хотите увидеть реальный distributed torch, замените `image` на CPU-сборку PyTorch (проверьте актуальный тег на Docker Hub — образы весят 1.5–3 ГБ) и подставьте минимальный тренировочный скрипт. Обязательно предзагрузите образ:

```bash
docker pull <cpu-pytorch-image>
kind load docker-image <cpu-pytorch-image> --name kubeflow
```

Иначе три ноды kind будут тянуть его независимо друг от друга.

### 8.7 Вопросы для самопроверки

1. Чем `TrainingRuntime` отличается от `ClusterTrainingRuntime` и когда namespaced-вариант предпочтительнее? (Подсказка в описании CRD: при namespaced-runtime TrainJob обязан жить в том же namespace.)
2. Что делает `spec.suspend: true` и как это использовать для gang-scheduling / очередей?
3. Trainer живёт в `kubeflow-system`, а Katib — в `kubeflow`. Какие практические последствия у этого для NetworkPolicy, квот и RBAC на реальном кластере?

---

## 9. Бонус — Spark Operator (20 мин)

```bash
kubectl -n ml-workshop create serviceaccount spark
kubectl create clusterrolebinding spark-role --clusterrole=edit \
  --serviceaccount=ml-workshop:spark
```

`spark-pi.yaml`:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-pi
  namespace: ml-workshop
spec:
  type: Scala
  mode: cluster
  image: spark:3.5.3
  imagePullPolicy: IfNotPresent
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: "local:///opt/spark/examples/jars/spark-examples.jar"
  sparkVersion: "3.5.3"
  driver:
    cores: 1
    memory: "512m"
    serviceAccount: spark
  executor:
    instances: 2
    cores: 1
    memory: "512m"
```

```bash
kubectl apply -f spark-pi.yaml
kubectl -n ml-workshop get sparkapplication,pods -w
```

> Точный тег образа и имя jar зависят от версии Spark — сверьтесь с примерами в `applications/spark/spark-operator`. Задача бонуса не в том, чтобы посчитать π, а в том, чтобы увидеть в Headlamp `SparkApplication` рядом с TrainJob и Experiment как ещё одну batch-нагрузку с состоянием, о котором отчитывается Kubernetes.

Сделайте `ScheduledSparkApplication` с cron-расписанием и посмотрите, как оно отображается — плагин показывает расписания RecurringRun в человекочитаемом виде; сравните подход.

---

## 10. Разбор: что забрать с собой

### 10.1 Операторский чек-лист для ML-нагрузок

Когда «ML-задача не работает», проходите слои сверху вниз:

| Слой | Вопрос | Команда / место |
|---|---|---|
| 1. Приняли ли манифест | Отказал ли admission webhook? | `kubectl apply` output; `kubectl get validatingwebhookconfigurations` |
| 2. Есть ли CR | Объект создан? Что в `.status.conditions`? | Headlamp: детали CR |
| 3. Отреагировал ли контроллер | Появились ли дочерние объекты? | `ownerReferences`, Headlamp Map |
| 4. Разместился ли Pod | `Pending`/`Unschedulable`? | Pod conditions: `reason` + `message` |
| 5. Стартовал ли контейнер | `ImagePullBackOff`, `CreateContainerConfigError`? | Headlamp: Pod conditions |
| 6. Выжил ли | `OOMKilled`, `Error`, exit code | `lastState.terminated` |
| 7. Дошли ли результаты | Метрики/артефакты попали куда надо? | sidecar-логи, `pipelineRoot`, `status.observation` |

Ключевая идея: слои 1–6 — это чистый Kubernetes и там ML-дашборд вам не помощник; слой 7 — единственный, где нужен ML-специфичный контекст.

### 10.2 Факты, которые ломают людям день

1. **CRD не успевает стать Established** → `no matches for kind` на первом apply. Просто повторите.
2. **Namespace-раздвоение**: Trainer в `kubeflow-system`, Katib/KFP в `kubeflow`. Проверяйте `-n` прежде, чем паниковать.
3. **Метка namespace `katib.kubeflow.org/metrics-collector-injection=enabled`** — без неё Trial выполняется, но метрик не будет. Симптом «всё зелёное, результата нет».
4. **`runtimeRef` в TrainJob immutable** — опечатку не поправить, только пересоздать.
5. **Immutable-поля при апгрейде** (`spec.selector` у Deployment): апстрим прямо предписывает удалить ресурс и переприменить. Это ожидаемое поведение, а не поломка.
6. **`NetworkPolicy` на под, который сам обслуживает admission webhook, должна отдельно открывать webhook-порт всем источникам.** kube-apiserver — не под, он не матчится через `podSelector`/`namespaceSelector`, поэтому обычные правила «разрешить из своего namespace» его не покрывают. Симптом — вызов webhook висит до таймаута, а не падает мгновенно. Общий паттерн для любого self-hosted admission-контроллера, не только KFP (см. п. 4.2).

### 10.3 Паттерн, который переносится дальше

Kubeflow — частный случай. Любая CRD-насыщенная платформа (Flux, Crossplane, Strimzi, KubeVirt) моделирует домен через custom resources и даёт свой узкий дашборд. Оператору при этом нужно состояние *нижележащих* API-ресурсов и подов. Плагин к универсальному Kubernetes UI поверх CRD выводит это состояние, не заставляя переключаться между несвязанными инструментами. Если у вас в компании есть своя платформа с CRD — это готовый рецепт, а плагин Kubeflow (Apache 2.0, разрабатывается в рамках Kubernetes SIG UI) — рабочий референс по коду.

---

## 11. Очистка

```bash
kind delete cluster --name kubeflow
docker system prune -af --volumes    # ⚠️ снесёт ВСЕ неиспользуемые образы, не только наши
rm -rf ~/kubeflow-workshop
```

Если хотите сохранить кластер до следующего раза, но освободить память:

```bash
kubectl -n kubeflow scale deploy --all --replicas=0
docker stop $(docker ps -q --filter name=kubeflow-)
# поднять обратно: docker start ...
```

---

## Приложение A — Troubleshooting

| Симптом | Причина | Решение |
|---|---|---|
| `no matches for kind "..."` | CRD ещё не Established | Повторить apply (retry-цикл) |
| Массовый `CrashLoopBackOff`, `too many open files` | Лимиты inotify | `sysctl` из п. 2.3, пересоздать кластер |
| `no matching manifest for linux/arm64` | ARM64 не поддержан | x86-хост или эмуляция |
| `toomanyrequests` от registry | Лимит Docker Hub | `docker login` + `regcred` (п. 2.4) |
| Поды `Pending`, `Insufficient cpu/memory` | Ноутбук кончился | Уменьшить `parallelTrialCount`, `replicas`, requests |
| Webhook TLS error при apply TrainJob | cert-manager не проставил caBundle | Дождаться `jsonpath={.webhooks[0].clientConfig.caBundle}` (п. 4.4) |
| `field is immutable` при apply | Immutable-поле изменилось между версиями | `kubectl delete <resource>` и переприменить |
| `workflow-controller` в `CrashLoopBackOff` | Не применены cluster-scoped ресурсы → нет CRD Argo | `kustomize build .../env/cert-manager/cluster-scoped-resources \| kubectl apply -f -` (п. 4.2, шаг 1) |
| `cache-server` вечно в `ContainerCreating`, `FailedMount` для `webhook-server-tls` | `cache-deployer` не создал секрет | Выдать Certificate вручную или отскейлить `cache-server` в 0 (врезка в п. 4.2) |
| `unable to find one of 'kustomization.yaml' ... in directory` | Указан каталог-контейнер под-оверлеев, а не сам оверлей | Проверить: `ls <dir>/kustomization.yaml`. Для Trainer правильный путь — `applications/trainer/overlays` |
| CRD и runtimes встали, а контроллеров нет | Один из `kustomize build` в цепочке упал, но остальные отработали | Читать вывод целиком: `kubectl apply` не останавливает скрипт на ошибке предыдущей команды |
| PVC `Pending` | Нет default StorageClass | В kind это `standard` (local-path). Проверить: `kubectl get sc` |
| Headlamp не видит разделы Kubeflow | API-группы не найдены / плагин не загрузился | `kubectl api-resources --api-group=...`; перезапустить Headlamp; проверить путь плагина |
| KFP UI пустой, а CR есть | `ml-pipeline` не поднялся | `kubectl -n kubeflow logs deploy/ml-pipeline` |
| Под/сервис недоступен с **другого** узла (таймаут, не `refused`); в пределах одного узла всё ок | Docker 27+, `FORWARD` на хосте = `DROP`, транзитная маршрутизация pod-подсетей через узел не разрешена | Правило в `DOCKER-USER` + systemd-юнит (п. 3.2) |
| `Pipeline` создался, `PipelineVersion` виснет: `failed calling webhook ... Client.Timeout exceeded while awaiting headers` на `mutate-pipelineversion` | `NetworkPolicy ml-pipeline` не пускает kube-apiserver (не под) к webhook-порту 8443 — баг апстрима | Live-патч + [issue #3580](https://github.com/kubeflow/community-distribution/issues/3580)/[PR #3581](https://github.com/kubeflow/community-distribution/pull/3581) (п. 4.2) |

Полезное на каждый день:

```bash
# Всё нездоровое разом
kubectl get pods -A --field-selector=status.phase!=Running,status.phase!=Succeeded

# События по времени
kubectl get events -A --sort-by=.lastTimestamp | tail -30

# Кто сколько ест
kubectl top pods -A --sort-by=memory | head -20
```

---

## Приложение B — Источники и что читать дальше

- Статья, с которой всё началось: [Introducing the Headlamp plugin for Kubeflow](https://kubernetes.io/blog/2026/07/13/introducing-headlamp-plugin-for-kubeflow/) (kubernetes.io) / [перевод на Хабре](https://habr.com/ru/companies/vktech/articles/1062726/)
- [README плагина](https://github.com/headlamp-k8s/plugins/blob/main/kubeflow/README.md) — в т.ч. лёгкий CRD-only путь для оценки без контроллеров
- [kubeflow/manifests](https://github.com/kubeflow/manifests) — источник истины по установке; матрица версий компонентов и ресурсных требований в README
- [Compile a Pipeline → Kubernetes Native API Mode](https://www.kubeflow.org/docs/components/pipelines/user-guides/core-functions/compile-a-pipeline/)
- [Katib: metrics collector](https://www.kubeflow.org/docs/components/katib/user-guides/metrics-collector/), [early stopping](https://www.kubeflow.org/docs/components/katib/user-guides/early-stopping/)
- [Kubeflow Trainer](https://trainer.kubeflow.org/en/latest/) — модель TrainJob/TrainingRuntime
- [Headlamp: расширение карты ресурсов](https://headlamp.dev/docs/latest/development/plugins/functionality/extending-the-map/) — если захотите написать плагин под свою платформу
- [kubeflow/community-distribution#3580](https://github.com/kubeflow/community-distribution/issues/3580) и [#3581](https://github.com/kubeflow/community-distribution/pull/3581) — баг с `NetworkPolicy ml-pipeline`, найденный и заведённый в рамках подготовки этого воркшопа (п. 4.2)

### Куда двигаться после воркшопа

1. **Полная платформа** — поднять `kustomize build example` на машине с 16 GB и увидеть Istio + Dex + Central Dashboard + Profiles; сравнить объём операционной сложности.
2. **GitOps** — положить `Pipeline`/`PipelineVersion`/`ClusterTrainingRuntime` в Git и накатывать через Flux/Argo CD. K8s-native режим KFP делает это осмысленным.
3. **Свой плагин Headlamp** — взять любую CRD-платформу из вашего стека и повторить паттерн.
4. **Наблюдаемость** — навесить Prometheus на `katib-controller`, `kubeflow-trainer-controller-manager`, `workflow-controller` и сформулировать SLI для платформы: «доля TrainJob, дошедших до Running за 5 минут».
