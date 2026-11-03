# arroyosas Helm chart

Deploys the arroyoSAXS stream processing stack:

| Component | Image | Purpose |
|---|---|---|
| `tiled` | `ghcr.io/bluesky/tiled` | Data server the operator reads from and writes results to |
| `lse-operator` | `ghcr.io/als-computing/arroyosas` | LSE reduction operator; serves results to the frontend over websocket |
| `beamline-sim` | `ghcr.io/als-computing/arroyosas` | Replays recorded tiled events for testing |
| `kvrocks` | `docker.io/apache/kvrocks` | Redis-compatible model store used by the operator |
| `mlflow` | `ghcr.io/mlflow/mlflow` | Tracking server for the operator's models |

Every component can be turned off with `<component>.enabled: false`.

## Install

From the registry:

```bash
helm upgrade --install arroyosas oci://ghcr.io/als-computing/charts/arroyosas \
  --version <version> -n arroyo --create-namespace -f my-values.yaml
```

From a checkout of this repo:

```bash
helm upgrade --install arroyosas ./charts/arroyosas -n arroyo --create-namespace -f my-values.yaml
```

`my-values.yaml` only needs the values you want to change. Maps are merged with the
defaults key by key, but lists and multi-line strings (`command`, `tiled.config`,
`lseOperator.blockConfig`) replace the default entirely. See all defaults with:

```bash
helm show values oci://ghcr.io/als-computing/charts/arroyosas --version <version>
```

## Versions

| Published from | Chart version | arroyosas image tag |
|---|---|---|
| Git tag `v1.2.0` | `1.2.0` | `v1.2.0` |
| Push to `main` | `<Chart.yaml version>-main.<run>` (prerelease) | `main` |

Prereleases are only installed when requested with `--version` or `--devel`. The
arroyosas image tag follows the chart's `appVersion` unless
`lseOperator.image.tag` / `beamlineSim.image.tag` are set.

## Tiled

By default the operator uses the tiled deployed by this chart. To use an external
tiled instead:

```yaml
tiled:
  enabled: false
lseOperator:
  tiled:
    uri: https://tiled.example.org
    existingSecret: my-tiled-key   # or apiKey: "..."
```

The operator subscribes to `lseOperator.tiled.subPath`, which must exist on the
tiled server before the operator starts.

### API keys

API keys are stored in Kubernetes Secrets and passed to pods with `secretKeyRef`.

| Setting | Result |
|---|---|
| Nothing set | A random key is generated into `<release>-arroyosas-tiled` and kept across upgrades |
| `tiled.apiKey` | That key is stored in `<release>-arroyosas-tiled` |
| `tiled.existingSecret` | Your Secret is used (key `tiled.existingSecretKey`); no Secret is created |
| `lseOperator.tiled.apiKey` / `existingSecret` | Same, for the operator's key to an external tiled |

Read the generated key:

```bash
kubectl -n arroyo get secret arroyosas-tiled -o jsonpath='{.data.api-key}' | base64 -d
```

Pods read the key at startup, so restart them after changing it:

```bash
kubectl -n arroyo rollout restart deploy/arroyosas-tiled deploy/arroyosas-lse-operator
```

## Accessing services

All services are `ClusterIP`. Use port forwarding:

```bash
kubectl -n arroyo port-forward svc/arroyosas-tiled 8000:8000          # http://localhost:8000/ui/
kubectl -n arroyo port-forward svc/arroyosas-mlflow 5000:5000         # http://localhost:5000
kubectl -n arroyo port-forward svc/arroyosas-lse-operator 8021:8021   # websocket for the frontend
```

The frontend is not part of this chart. Run it from `frontend/` pointed at the
forwarded operator:

```bash
cd frontend && npm install
REACT_APP_WEBSOCKET_URL=ws://localhost:8021 npm start
```

## Persistence

Persistence is disabled by default and every component uses an `emptyDir`, so data
is lost when pods restart. Enable it per component:

```yaml
tiled:
  persistence:
    enabled: true
    size: 50Gi
    storageClassName: ""   # cluster default
    existingClaim: ""      # or reuse a PVC
```

The same settings exist for `beamlineSim` (event logs mounted at
`/app/tiled_event_logs`), `kvrocks` and `mlflow`. The beamline sim has nothing to
replay until event logs are placed in that volume.

## Values

| Key | Default | Description |
|---|---|---|
| `nameOverride` / `fullnameOverride` | `""` | Override resource names |
| `tiled.enabled` | `true` | Deploy tiled |
| `tiled.image.repository` / `tag` | `ghcr.io/bluesky/tiled` / `0.2.8` | Tiled image |
| `tiled.service.port` | `8000` | Tiled service port |
| `tiled.apiKey` | `""` | API key; generated if empty |
| `tiled.existingSecret` / `existingSecretKey` | `""` / `api-key` | Pre-created Secret holding the API key |
| `tiled.config` | catalog at `/data` | Tiled `config.yml` contents |
| `tiled.persistence.*` | disabled, `10Gi` | Volume for `/data` |
| `beamlineSim.enabled` | `true` | Deploy the beamline simulator |
| `beamlineSim.image.tag` | `""` | Image tag; defaults to `appVersion` |
| `beamlineSim.command` | `tiled_event_sim_cli` | Container command |
| `beamlineSim.persistence.*` | disabled, `10Gi` | Volume for event logs |
| `lseOperator.enabled` | `true` | Deploy the LSE operator |
| `lseOperator.image.tag` | `""` | Image tag; defaults to `appVersion` |
| `lseOperator.service.port` | `8021` | Websocket port for results |
| `lseOperator.tiled.uri` | `""` | External tiled URI; empty uses the bundled tiled |
| `lseOperator.tiled.apiKey` | `""` | Key for an external tiled |
| `lseOperator.tiled.existingSecret` / `existingSecretKey` | `""` / `api-key` | Pre-created Secret for an external tiled's key |
| `lseOperator.tiled.subPath` | `beamlines/bl733/dylan_test` | Tiled path the operator listens on |
| `lseOperator.mlflowTrackingUri` | `""` | MLflow URI; empty uses the bundled mlflow |
| `lseOperator.blockConfig` | see `values.yaml` | arroyopy block config, rendered with `tpl` |
| `kvrocks.enabled` | `true` | Deploy kvrocks |
| `kvrocks.image.tag` | `latest` | Kvrocks image tag |
| `kvrocks.service.port` | `6666` | Kvrocks service port |
| `kvrocks.persistence.*` | disabled, `5Gi` | Kvrocks data volume |
| `mlflow.enabled` | `true` | Deploy mlflow |
| `mlflow.image.tag` | `latest` | MLflow image tag |
| `mlflow.service.port` | `5000` | MLflow service port |
| `mlflow.command` | sqlite backend in `/mlflow` | Container command |
| `mlflow.persistence.*` | disabled, `10Gi` | MLflow database and artifacts volume |

Every component also accepts `image.pullPolicy` and `service.type`.

## Local testing with kind

On Apple Silicon, the published arroyosas image is amd64-only and crashes under
emulation. Build it locally and load it into kind:

```bash
KIND_EXPERIMENTAL_PROVIDER=podman kind create cluster --name arroyo
podman build -t localhost/arroyosas:dev .
podman save localhost/arroyosas:dev -o /tmp/arroyosas-dev.tar
KIND_EXPERIMENTAL_PROVIDER=podman kind load image-archive /tmp/arroyosas-dev.tar --name arroyo

helm upgrade --install arroyosas ./charts/arroyosas -n arroyo --create-namespace \
  --kube-context kind-arroyo \
  --set lseOperator.image.repository=localhost/arroyosas \
  --set lseOperator.image.tag=dev --set lseOperator.image.pullPolicy=Never \
  --set beamlineSim.image.repository=localhost/arroyosas \
  --set beamlineSim.image.tag=dev --set beamlineSim.image.pullPolicy=Never
```
