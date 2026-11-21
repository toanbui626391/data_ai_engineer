# Simulating Amazon ECS Applications Locally

The Solution Architect rule for developing ECS applications without AWS access is simple: **your application code should never know it is running on ECS.**

When you enforce ECS's runtime contracts locally with **Docker Compose** and **LocalStack**, promoting your app to AWS ECS requires **zero code changes**—only swapping environment configuration and deploying the exact same Docker image.

---

## 1. The 3-Layer Local Simulation Model

```text
LOCAL (docker-compose.yml)                  AWS PRODUCTION (ECS)
──────────────────────────                  ────────────────────
Your Docker Image           ──same image──▶ Your Docker Image (Amazon ECR)
LocalStack (S3, SQS, etc.)  ──swap env───▶  Real AWS Services (+ ECS Task Role)
Real OSS Containers         ──swap env───▶  Managed Engines (Amazon OpenSearch, RDS)
.env file                   ──swap env───▶  ECS Secrets Manager / SSM injection
```

1. **Your Application Code (Simulates the ECS Task)**: Runs in a Docker container via Docker Compose with matching CPU/RAM caps, read-only root filesystem, health check, and `SIGTERM` grace period.
2. **AWS-Managed Services (`S3`, `SQS`, `DynamoDB`, `Secrets Manager`)**: Simulated by **LocalStack** in the same Compose network, routed transparently via `AWS_ENDPOINT_URL`.
3. **Open-Source Engines (`OpenSearch`, `Postgres`, `Redis`)**: Run their **real official Docker images** in Compose for 100% behavioral parity (no emulation needed).

---

## 2. Six ECS Runtime Contracts to Simulate Locally

| ECS Production Behavior | How to Simulate Locally | Why It Prevents Deployment Surprises |
|---|---|---|
| **1. Strict Fargate CPU/RAM & Ephemeral Disk** | Set `deploy.resources.limits` (`cpus`, `memory`) and `read_only: true` + `tmpfs: [/tmp]`. | Catches `OOMKilled` crashes and accidental writes to container disk (Fargate wipes disk on task stop). |
| **2. IAM Task Role Credentials** | Use the default AWS SDK chain (`boto3.client("s3")` with no hardcoded keys); pass `AWS_ENDPOINT_URL=http://localstack:4566` + dummy keys locally. | Modern AWS SDKs honor `AWS_ENDPOINT_URL` natively. On ECS, omitting those env vars automatically uses real AWS + the **Task Role** with zero `if local:` code. |
| **3. Secrets Manager / SSM Injection** | Load secrets from a gitignored `.env` via `env_file`. App reads `os.environ[...]`. | On ECS, the **ECS Agent** (via Task Execution Role) injects Secrets Manager values as env vars before startup—identical interface to `.env`. |
| **4. CloudWatch Logs & ALB Health Checks** | Log structured JSON to `stdout`/`stderr` only; expose `GET /health` (`200 OK`) and configure Compose `healthcheck`. | ECS `awslogs` driver captures `stdout`/`stderr` automatically; ALB targets rely on `GET /health`. |
| **5. Graceful Shutdown (`SIGTERM`)** | Set `stop_grace_period: 30s` and test with `docker compose stop app`. | ECS sends `SIGTERM` before `SIGKILL` during rolling deploys, scale-in, and **Fargate Spot** interruptions. |
| **6. CPU Architecture Parity (`arm64` vs `amd64`)** | Pin `platforms` (`linux/arm64` or `linux/amd64`) to match your target ECS `runtimePlatform.cpuArchitecture`. | Prevents `exec format error` when images built on Apple Silicon (`arm64`) are deployed to `x86_64` Fargate tasks. *(Tip: target `ARM64` Graviton on Fargate for ~20% savings + Mac parity.)* |

> **Sidecars & ECS Metadata**:
> - To simulate multiple containers inside one `awsvpc` Task (sharing `localhost`), attach sidecars using `network_mode: "service:app"`.
> - If your code explicitly queries `$ECS_CONTAINER_METADATA_URI_V4`, add AWS's official local sidecar image: `amazon/amazon-ecs-local-container-endpoints`.

---

## 3. Reference `docker-compose.yml`

```yaml
services:
  app:
    build:
      context: .
      platforms: ["linux/arm64"]                 # Match ECS runtimePlatform.cpuArchitecture
    ports:
      - "8000:8000"
    env_file:
      - .env                                     # Mirrors ECS Task Definition secrets/env
    environment:
      - AWS_REGION=us-east-1
      - AWS_ENDPOINT_URL=http://localstack:4566  # Omitted in AWS ECS
      - AWS_ACCESS_KEY_ID=test                   # Omitted in AWS ECS (Task Role used)
      - AWS_SECRET_ACCESS_KEY=test               # Omitted in AWS ECS (Task Role used)
      - OPENSEARCH_URL=http://opensearch:9200    # Swapped to managed OpenSearch endpoint in ECS
    deploy:
      resources:
        limits:
          cpus: "1.0"                            # Mirrors Fargate 1024 CPU units
          memory: 2048M                          # Mirrors Fargate 2048 MB memory
    read_only: true                              # Enforces stateless container discipline
    tmpfs:
      - /tmp
    stop_grace_period: 30s                       # Mirrors ECS stopTimeout (SIGTERM -> SIGKILL)
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 5s
      retries: 3
    depends_on:
      - localstack
      - opensearch

  localstack:
    image: localstack/localstack:latest
    ports:
      - "4566:4566"
    environment:
      - SERVICES=s3,sqs,secretsmanager

  opensearch:
    image: opensearchproject/opensearch:2.17.1
    environment:
      - discovery.type=single-node
      - DISABLE_SECURITY_PLUGIN=true
      - OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m
    ports:
      - "9200:9200"
```

---

## 4. Direct Mapping: `docker-compose.yml` -> ECS Task Definition

| `docker-compose.yml` Field | ECS Task Definition / Service Equivalent |
|---|---|
| `image` / `build` | `containerDefinitions[].image` (pushed to Amazon ECR) |
| `deploy.resources.limits.cpus` / `memory` | Task-level `cpu` (`1024`) and `memory` (`2048`) |
| `ports` | `containerDefinitions[].portMappings` + ALB Target Group (`ip` target type) |
| `environment` | `containerDefinitions[].environment` |
| `env_file` (sensitive values) | `containerDefinitions[].secrets` (ARNs from Secrets Manager / SSM) |
| `read_only: true` | `containerDefinitions[].readonlyRootFilesystem: true` |
| `stop_grace_period` | `containerDefinitions[].stopTimeout` |
| `healthcheck` | `containerDefinitions[].healthCheck` + ALB Target Group health check |
| Compose service DNS (`http://api:8000`) | **ECS Service Connect** discovery name |

---

## 5. Day-1 AWS Cutover Checklist

When ECS access becomes available:
1. Build and push the tested Docker image to **Amazon ECR** (matching target CPU architecture).
2. Register the **ECS Task Definition** with identical `cpu`, `memory`, `portMappings`, `readonlyRootFilesystem`, `stopTimeout`, and `awslogs` log configuration.
3. Move `.env` secrets to **Secrets Manager / SSM** (`secrets` block), set production endpoints (`OPENSEARCH_URL`), and **remove** `AWS_ENDPOINT_URL` plus dummy local AWS keys.
4. Attach a least-privilege **Task Role** (app permissions: S3/SQS/OpenSearch) and **Task Execution Role** (ECR pull, Secrets Manager read, CloudWatch Logs write).
5. Deploy behind an **ECS Service** + **ALB** (or trigger via `RunTask` / EventBridge for batch workers).
