# Amazon ECS Key Concepts (Solution Architect Guide)

**Amazon Elastic Container Service (ECS)** is AWS's native, fully managed container orchestrator. Unlike Kubernetes (EKS), ECS has **no control-plane fee** and minimal operational complexity while integrating natively with AWS IAM, VPC, ALB, and CloudWatch.

---

## 1. Core Building Blocks (The 4-Layer Hierarchy)

```text
Cluster (Logical boundary + Capacity: Fargate / EC2)
 └── Service (Maintains desired Task count, Load Balancing, Auto Scaling)
      └── Task (A running instance of a Task Definition — equivalent to a K8s Pod)
           └── Task Definition (Blueprint: Images, CPU/RAM, IAM Roles, Ports, Volumes)
```

| Concept | What It Is | K8s Equivalent | Solution Architect Notes |
|---|---|---|---|
| **Task Definition** | Immutable versioned JSON blueprint (`family:revision`) defining 1+ containers, CPU/RAM, ports, secrets, and IAM roles. | `PodSpec` | Max 10 containers per task; usually 1 app container + optional sidecars (Envoy, Datadog, FluentBit). |
| **Task** | A single running instantiation of a Task Definition. Containers in a Task share the same lifecycle, IP (`awsvpc`), and local volumes. | `Pod` | Run directly via `RunTask` API / EventBridge / Step Functions for batch jobs, ETL, or RAG ingestion workers. |
| **Service** | Ensures $N$ copies of a Task stay running across AZs, replaces unhealthy tasks, attaches to ALB/NLB, and runs rolling or Blue/Green deploys. | `Deployment` + `Service` | Use for 24/7 APIs, web apps, and queue consumers. |
| **Cluster** | Logical grouping of Tasks, Services, and compute capacity within a Region. | `Cluster` / `Namespace` | Can mix **Fargate** and **EC2** tasks inside the same cluster via Capacity Providers. |

---

## 2. Compute Options & Capacity Providers

| Launch Type / Feature | How It Works | When a Solution Architect Chooses It |
|---|---|---|
| **AWS Fargate (Serverless)** | AWS manages the underlying micro-VMs. You specify only Task vCPU (0.25–16 vCPU) and Memory (0.5–120 GB). | **Default choice** for stateless APIs, microservices, and spiky/batch workers—zero OS patching or cluster bin-packing. |
| **EC2 Launch Type** | Tasks run on your own EC2 Auto Scaling Groups running the ECS Agent + Docker/containerd. | **GPU workloads** (LLM inference/embedding), massive steady-state fleets using EC2 Savings Plans, or custom kernel/daemon requirements. |
| **ECS Anywhere** | Runs the ECS agent on on-premises servers or edge VMs managed by the AWS ECS control plane. | Hybrid-cloud migrations or local data-residency requirements without adopting Kubernetes. |
| **Capacity Providers** | Rules defining how Tasks allocate across capacity pools (e.g., `base=2` on `FARGATE`, `weight=1` on `FARGATE`, `weight=4` on `FARGATE_SPOT`). | **Cost optimization**: Keep baseline production traffic on On-Demand Fargate and burst 80% of scale-out or batch tasks onto **Fargate Spot** (up to 70% cheaper). |

---

## 3. Critical SA Distinctions: IAM, Networking & Traffic

### A. Task Execution Role vs. Task Role (Classic SA Exam & Design Topic)
- **Task Execution Role** *(for the ECS/Fargate infrastructure)*: Grants the **ECS Agent** permission to start your container—pulling images from **ECR**, decrypting secrets from **Secrets Manager / SSM Parameter Store**, and shipping container logs to **CloudWatch Logs**.
- **Task Role** *(for your application code)*: Grants **your code inside the container** least-privilege permissions to call AWS services (e.g., reading S3 buckets, querying OpenSearch/DynamoDB, invoking Bedrock). Every task type gets its own IAM role—never attach permissions to the shared EC2 host role.

### B. Networking Modes (`awsvpc`)
- **`awsvpc` Mode (Standard & Required on Fargate)**: Assigns a dedicated **Elastic Network Interface (ENI)** and private VPC IP to *each Task*.
  - **Pros**: Each service gets its own **Security Group** (zero-trust container-level firewalling) and shows up individually in VPC Flow Logs.
  - **SA Watchout**: High task density can exhaust subnet IP addresses or EC2 ENI limits (mitigate with oversized `/19` or `/20` private subnets and **ENI Trunking** on EC2).
- **`bridge` / `host` (EC2 only, Legacy)**: Shares the host EC2 IP via dynamic port mapping; avoided in modern architectures except when extreme task-per-instance density is required.

### C. Traffic Routing & Service Discovery
- **North-South (External → ECS)**: **Application Load Balancer (ALB)** (L7 HTTP/gRPC routing directly to Task ENI IPs via `ip` target type) or **Network Load Balancer (NLB)** (high-throughput TCP/UDP).
- **East-West (Service → Service)**:
  - **ECS Service Connect (Recommended)**: Built-in managed Envoy proxy sidecar providing DNS names (`http://payments-api:8080`), automatic retries, timeouts, and golden-signal telemetry without paying for internal ALBs.
  - **AWS Cloud Map**: DNS-based (`A` / `SRV` records in Route 53 private hosted zones) service discovery.

---

## 4. Auto Scaling & Storage

- **Two-Layer Auto Scaling**:
  1. **Service Auto Scaling** (scales *Tasks*): Target tracking on `ECSServiceAverageCPUUtilization`, `ALBRequestCountPerTarget`, or custom CloudWatch metrics (e.g., **SQS `ApproximateNumberOfMessagesVisible` per task** for async workers).
  2. **Cluster Auto Scaling** (scales *EC2 hosts*): Managed automatically on Fargate; on EC2, **ASG Capacity Providers** calculate required instances so pending tasks never get stuck without host capacity.
- **Storage**:
  - **Ephemeral Storage**: 20 GB free per Fargate Task (expandable up to 200 GB) for scratch files/model weights downloaded at startup; wiped when the Task stops.
  - **Persistent Shared Storage**: Native **Amazon EFS** volume mounts inside the Task Definition (secured via EFS Access Points + IAM) for shared files across tasks and AZs.

---

## 5. Solution Architect Decision Matrix: AWS ECS vs. Amazon EKS (Kubernetes)

| Dimension | Choose **Amazon ECS** When... | Choose **Amazon EKS (Kubernetes)** When... |
|---|---|---|
| **Team & Operations** | Lean platform/DevOps team wants low operational overhead and fast time-to-production. | Dedicated platform team already standardizes on K8s tooling (Helm, ArgoCD, Karpenter, Istio, Prometheus). |
| **Portability Strategy** | AWS-first architecture leveraging native IAM, ALB, CloudWatch, Step Functions, EventBridge. | Strict multi-cloud / hybrid portability requirement or running complex CNCF open-source operators (Ray, Kubeflow, Spark-on-K8s). |
| **Control Plane Cost & Upgrades** | \$0/month control plane; zero control-plane version upgrade cycles. | \$0.10/hour (\$73/mo) per cluster + mandatory K8s version upgrades every 12–14 months (or Extended Support fees). |
| **AI / Data Engineering Fit** | Ideal for RAG APIs, event-driven document connectors (Confluence/SharePoint `RunTask` workers), and straightforward GPU inference endpoints. | Better for large distributed training (Kubeflow/Ray), complex multi-step ML DAGs (Argo Workflows), or dynamic GPU bin-packing (Karpenter). |
