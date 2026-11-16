# GroveMQ 部署指南

本文档提供GroveMQ MQTT Broker集群的完整部署指南，涵盖本地开发、测试和生产环境部署。

---

## 目录

- [快速开始](#快速开始)
- [环境要求](#环境要求)
- [本地部署](#本地部署)
- [Docker部署](#docker部署)
- [Kubernetes部署](#kubernetes部署)
- [生产环境配置](#生产环境配置)
- [监控部署](#监控部署)
- [安全配置](#安全配置)
- [性能调优](#性能调优)

---

## 快速开始

### 5分钟快速体验

```bash
# 1. 克隆代码
git clone https://github.com/RiemannChow/GroveMQ.git
cd GroveMQ

# 2. 构建
go build -o bin/grovemq ./cmd/grovemq

# 3. 启动集群
./scripts/start-cluster.sh

# 4. 验证
./scripts/health-check-simple.sh

# 5. 测试
./bin/cluster-test
```

---

## 环境要求

### 最低要求

| 组件 | 版本 | 说明 |
|------|------|------|
| Go | 1.21+ | 编译器 |
| CPU | 2 核心 | 最小配置 |
| 内存 | 2GB | 最小配置 |
| 磁盘 | 10GB | 日志和数据 |
| 操作系统 | Linux/macOS | 推荐Linux |

### 推荐配置

| 组件 | 版本 | 说明 |
|------|------|------|
| Go | 1.21+ | 最新稳定版 |
| CPU | 4+ 核心 | 生产环境 |
| 内存 | 8GB+ | 生产环境 |
| 磁盘 | 100GB+ SSD | 高性能存储 |
| 操作系统 | Ubuntu 22.04 LTS | 推荐 |

### 依赖工具

```bash
# macOS
brew install go git mosquitto

# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y golang git mosquitto-clients

# CentOS/RHEL
sudo yum install -y golang git mosquitto
```

---

## 本地部署

### 单节点部署

```bash
# 1. 编译
go build -o bin/grovemq ./cmd/grovemq

# 2. 运行
./bin/grovemq

# 3. 测试连接
mosquitto_pub -h localhost -p 1883 -t "test/topic" -m "Hello GroveMQ"
mosquitto_sub -h localhost -p 1883 -t "test/topic"
```

### 3节点集群部署

```bash
# 使用自动化脚本
./scripts/start-cluster.sh

# 或手动启动
# Node 1
NODE_ID=node1 MQTT_PORT=:1883 GRPC_PORT=:8081 \
  METRICS_PORT=:8082 DASHBOARD_PORT=:18083 \
  ./bin/grovemq &

# Node 2
NODE_ID=node2 MQTT_PORT=:1884 GRPC_PORT=:8083 \
  METRICS_PORT=:8084 DASHBOARD_PORT=:18084 \
  PEER_NODES=localhost:8081 \
  ./bin/grovemq &

# Node 3
NODE_ID=node3 MQTT_PORT=:1885 GRPC_PORT=:8085 \
  METRICS_PORT=:8086 DASHBOARD_PORT=:18085 \
  PEER_NODES=localhost:8081,localhost:8083 \
  ./bin/grovemq &
```

### 配置文件部署

1. **生成配置文件**:
```bash
./bin/grovemq -generate-config config.yaml
```

2. **编辑配置** (`config.yaml`):
```yaml
broker:
  node_id: "node1"
  mqtt_port: ":1883"
  grpc_port: ":8081"
  metrics_port: ":8082"
  dashboard_port: ":18083"
  auth:
    enabled: true
    users:
      - username: "admin"
        password: "your-secure-password"
        algorithm: "bcrypt"
        enabled: true
```

3. **启动**:
```bash
./bin/grovemq -config config.yaml
```

---

## Docker部署

### 构建镜像

```bash
# 创建 Dockerfile
cat > Dockerfile << 'EOF'
FROM golang:1.25-alpine AS builder

WORKDIR /build
COPY . .
RUN go build -o grovemq ./cmd/grovemq

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /build/grovemq .

EXPOSE 1883 8081 8082 18083

CMD ["./grovemq"]
EOF

# 构建
docker build -t grovemq:latest .
```

### 单容器运行

```bash
docker run -d \
  --name grovemq \
  -p 1883:1883 \
  -p 8082:8082 \
  -p 18083:18083 \
  grovemq:latest
```

### Docker Compose集群

创建 `docker-compose.yml`:

```yaml
version: '3.8'

services:
  node1:
    image: grovemq:latest
    container_name: grovemq-node1
    environment:
      - NODE_ID=node1
      - MQTT_PORT=:1883
      - GRPC_PORT=:8081
      - METRICS_PORT=:8082
      - DASHBOARD_PORT=:18083
    ports:
      - "1883:1883"
      - "8081:8081"
      - "8082:8082"
      - "18083:18083"
    networks:
      - grovemq-net

  node2:
    image: grovemq:latest
    container_name: grovemq-node2
    environment:
      - NODE_ID=node2
      - MQTT_PORT=:1883
      - GRPC_PORT=:8081
      - METRICS_PORT=:8082
      - DASHBOARD_PORT=:18083
      - PEER_NODES=node1:8081
    ports:
      - "1884:1883"
      - "8083:8081"
      - "8084:8082"
      - "18084:18083"
    networks:
      - grovemq-net
    depends_on:
      - node1

  node3:
    image: grovemq:latest
    container_name: grovemq-node3
    environment:
      - NODE_ID=node3
      - MQTT_PORT=:1883
      - GRPC_PORT=:8081
      - METRICS_PORT=:8082
      - DASHBOARD_PORT=:18083
      - PEER_NODES=node1:8081,node2:8081
    ports:
      - "1885:1883"
      - "8085:8081"
      - "8086:8082"
      - "18085:18083"
    networks:
      - grovemq-net
    depends_on:
      - node1
      - node2

networks:
  grovemq-net:
    driver: bridge
```

启动：
```bash
docker-compose up -d
```

---

## Kubernetes部署

### 准备工作

```bash
# 创建命名空间
kubectl create namespace grovemq

# 创建配置映射
kubectl create configmap grovemq-config \
  --from-file=config.yaml \
  -n grovemq
```

### StatefulSet部署

创建 `k8s-deployment.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: grovemq-headless
  namespace: grovemq
spec:
  clusterIP: None
  selector:
    app: grovemq
  ports:
    - name: mqtt
      port: 1883
      targetPort: 1883
    - name: grpc
      port: 8081
      targetPort: 8081
    - name: metrics
      port: 8082
      targetPort: 8082
# The client-facing L4 MQTT load balancer is delivered as k8s/service.yaml.
# Apply it with: kubectl apply -f k8s/service.yaml
# (type: LoadBalancer, sessionAffinity: ClientIP, externalTrafficPolicy: Local;
#  see docs/design/LOAD_BALANCING.md §6.1). Single source of truth, no drift.
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: grovemq
  namespace: grovemq
spec:
  serviceName: grovemq-headless
  replicas: 3
  selector:
    matchLabels:
      app: grovemq
  template:
    metadata:
      labels:
        app: grovemq
    spec:
      containers:
      - name: grovemq
        image: grovemq:latest
        imagePullPolicy: IfNotPresent
        env:
        - name: NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: MQTT_PORT
          value: ":1883"
        - name: GRPC_PORT
          value: ":8081"
        - name: METRICS_PORT
          value: ":8082"
        - name: DASHBOARD_PORT
          value: ":18083"
        ports:
        - containerPort: 1883
          name: mqtt
        - containerPort: 8081
          name: grpc
        - containerPort: 8082
          name: metrics
        - containerPort: 18083
          name: dashboard
        resources:
          requests:
            cpu: "500m"
            memory: "512Mi"
          limits:
            cpu: "2000m"
            memory: "2Gi"
        livenessProbe:
          tcpSocket:
            port: 1883
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          tcpSocket:
            port: 1883
          initialDelaySeconds: 5
          periodSeconds: 5
```

部署：
```bash
kubectl apply -f k8s-deployment.yaml
```

验证：
```bash
# 检查Pod状态
kubectl get pods -n grovemq

# 检查服务
kubectl get svc -n grovemq

# 查看日志
kubectl logs -f grovemq-0 -n grovemq
```

---

## 生产环境配置

### 系统优化

```bash
# /etc/sysctl.conf
# 增加文件描述符限制
fs.file-max = 2097152
fs.nr_open = 2097152

# TCP优化
net.ipv4.tcp_max_syn_backlog = 8192
net.core.somaxconn = 8192
net.core.netdev_max_backlog = 8192

# 连接追踪
net.netfilter.nf_conntrack_max = 1048576
net.nf_conntrack_max = 1048576

# 应用配置
sudo sysctl -p
```

```bash
# /etc/security/limits.conf
* soft nofile 1048576
* hard nofile 1048576
* soft nproc unlimited
* hard nproc unlimited
```

### 日志配置

```bash
# 使用logrotate管理日志
cat > /etc/logrotate.d/grovemq << 'EOF'
/var/log/grovemq/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0640 grovemq grovemq
    sharedscripts
    postrotate
        systemctl reload grovemq > /dev/null 2>&1 || true
    endscript
}
EOF
```

### Systemd服务

创建 `/etc/systemd/system/grovemq.service`:

```ini
[Unit]
Description=GroveMQ MQTT Broker
After=network.target

[Service]
Type=simple
User=grovemq
Group=grovemq
WorkingDirectory=/opt/grovemq
Environment="NODE_ID=node1"
Environment="MQTT_PORT=:1883"
Environment="GRPC_PORT=:8081"
Environment="METRICS_PORT=:8082"
Environment="DASHBOARD_PORT=:18083"
ExecStart=/opt/grovemq/bin/grovemq -config /etc/grovemq/config.yaml
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=grovemq

# 资源限制
LimitNOFILE=1048576
LimitNPROC=unlimited

[Install]
WantedBy=multi-user.target
```

启用服务：
```bash
sudo systemctl daemon-reload
sudo systemctl enable grovemq
sudo systemctl start grovemq
sudo systemctl status grovemq
```

---

## 监控部署

### Prometheus配置

创建 `prometheus.yml`:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "monitoring/prometheus-alerts.yml"

scrape_configs:
  - job_name: 'grovemq'
    static_configs:
      - targets:
        - 'localhost:8082'
        - 'localhost:8084'
        - 'localhost:8086'
        labels:
          cluster: 'grovemq-prod'
```

启动：
```bash
prometheus --config.file=prometheus.yml
```

### Grafana配置

```bash
# 安装
brew install grafana  # macOS
sudo apt-get install grafana  # Ubuntu

# 启动
brew services start grafana  # macOS
sudo systemctl start grafana  # Linux

# 访问 http://localhost:3000
# 默认账号: admin/admin

# 添加数据源
# Configuration → Data Sources → Add data source
# 选择 Prometheus
# URL: http://localhost:9090

# 导入仪表板
# Dashboard → Import
# 上传: monitoring/grafana-dashboard.json
```

### AlertManager配置

创建 `alertmanager.yml`:

```yaml
global:
  resolve_timeout: 5m
  smtp_smarthost: 'smtp.gmail.com:587'
  smtp_from: 'alerts@your-domain.com'
  smtp_auth_username: 'your-email@gmail.com'
  smtp_auth_password: 'your-app-password'

route:
  group_by: ['alertname', 'cluster', 'severity']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 12h
  receiver: 'email-notifications'
  routes:
    - match:
        severity: critical
      receiver: 'pagerduty'
      continue: true
    - match:
        severity: warning
      receiver: 'email-notifications'

receivers:
  - name: 'email-notifications'
    email_configs:
      - to: 'team@your-domain.com'
        headers:
          Subject: '[GroveMQ] {{ .GroupLabels.alertname }}'

  - name: 'pagerduty'
    pagerduty_configs:
      - service_key: 'your-pagerduty-key'
```

---

## 安全配置

### TLS/SSL配置

1. **生成证书**:
```bash
# 自签名证书（仅用于测试）
openssl req -x509 -newkey rsa:4096 \
  -keyout key.pem -out cert.pem \
  -days 365 -nodes \
  -subj "/CN=grovemq.local"
```

2. **配置GroveMQ** (TODO: 需要在代码中添加TLS支持):
```yaml
broker:
  mqtt_port: ":1883"
  mqtts_port: ":8883"
  tls:
    enabled: true
    cert_file: "/path/to/cert.pem"
    key_file: "/path/to/key.pem"
```

### 认证配置

编辑 `config.yaml`:

```yaml
broker:
  auth:
    enabled: true
    users:
      - username: "admin"
        password: "$2a$10$..."  # bcrypt hash
        algorithm: "bcrypt"
        enabled: true
      - username: "device001"
        password: "device-token-001"
        algorithm: "sha256"
        enabled: true
```

生成bcrypt密码：
```bash
# 使用在线工具或自己编写Go程序
# https://bcrypt-generator.com/
```

### 防火墙配置

```bash
# Ubuntu/Debian (ufw)
sudo ufw allow 1883/tcp  # MQTT
sudo ufw allow 8883/tcp  # MQTTS
sudo ufw allow 8081/tcp  # gRPC (仅内网)
sudo ufw allow 8082/tcp  # Metrics (仅内网)

# CentOS/RHEL (firewalld)
sudo firewall-cmd --permanent --add-port=1883/tcp
sudo firewall-cmd --permanent --add-port=8883/tcp
sudo firewall-cmd --reload
```

---

## 性能调优

### 连接数优化

```yaml
# config.yaml
broker:
  max_connections: 100000
  max_packet_size: 268435456  # 256MB
  keepalive_backoff: 0.75
```

### 内存优化

```bash
# 设置Go运行时
export GOGC=100  # GC触发百分比
export GOMAXPROCS=8  # 使用CPU核心数
```

### 网络优化

```bash
# TCP参数优化
sudo sysctl -w net.ipv4.tcp_tw_reuse=1
sudo sysctl -w net.ipv4.tcp_fin_timeout=30
sudo sysctl -w net.ipv4.tcp_keepalive_time=300
sudo sysctl -w net.ipv4.tcp_keepalive_probes=3
sudo sysctl -w net.ipv4.tcp_keepalive_intvl=30
```

### 磁盘I/O优化

```bash
# 使用SSD
# 禁用访问时间更新
mount -o noatime,nodiratime /dev/sda1 /mnt/data

# 调整I/O调度器
echo noop > /sys/block/sda/queue/scheduler
```

---

## 备份和恢复

### 数据备份

```bash
# 备份脚本
#!/bin/bash
BACKUP_DIR=/backup/grovemq/$(date +%Y%m%d)
mkdir -p $BACKUP_DIR

# 备份配置
cp /etc/grovemq/config.yaml $BACKUP_DIR/

# 备份日志
cp -r /var/log/grovemq $BACKUP_DIR/logs/

# 压缩
tar -czf $BACKUP_DIR.tar.gz $BACKUP_DIR
rm -rf $BACKUP_DIR
```

### 恢复

```bash
# 解压备份
tar -xzf /backup/grovemq/20250110.tar.gz -C /tmp/

# 恢复配置
cp /tmp/20250110/config.yaml /etc/grovemq/

# 重启服务
sudo systemctl restart grovemq
```

---

## 故障排查

参见 [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) 获取详细的故障排查指南。

---

## 升级指南

### 滚动升级

```bash
# 1. 备份当前配置和数据
./scripts/backup.sh

# 2. 逐个节点升级
# 停止node3
systemctl stop grovemq@node3

# 更新二进制文件
cp new/grovemq /opt/grovemq/bin/

# 启动node3
systemctl start grovemq@node3

# 验证
./scripts/health-check-simple.sh

# 重复node2和node1
```

---

## 附录

### 端口列表

| 端口 | 协议 | 用途 | 访问控制 |
|------|------|------|---------|
| 1883 | MQTT | MQTT连接 | 公网 |
| 8883 | MQTTS | MQTT over TLS | 公网 |
| 8081 | gRPC | 集群通信 | 内网 |
| 8082 | HTTP | Prometheus指标 | 内网 |
| 18083 | HTTP | Dashboard | 内网 |

### 配置参数参考

| 参数 | 默认值 | 说明 |
|------|--------|------|
| NODE_ID | "local-node" | 节点唯一标识 |
| MQTT_PORT | ":1883" | MQTT监听端口 |
| GRPC_PORT | ":8081" | gRPC监听端口 |
| METRICS_PORT | ":8082" | 指标监听端口 |
| DASHBOARD_PORT | ":18083" | Dashboard端口 |
| PEER_NODES | "" | 集群对等节点 |

---

**文档版本**: v1.0
**最后更新**: 2025-10-11
**维护者**: GroveMQ团队
