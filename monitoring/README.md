# 监控和运维工具

本目录包含GroveMQ集群的监控、告警和运维工具。

---

## 📁 目录结构

```
monitoring/
├── grafana-dashboard.json     # Grafana仪表板配置
├── prometheus-alerts.yml      # Prometheus告警规则
└── README.md                  # 本文档

scripts/
├── health-check.sh            # 完整健康检查（bash 4+）
├── health-check-simple.sh     # 简化健康检查（兼容所有bash）
└── performance-test.sh        # 性能测试套件
```

---

## 🎯 快速开始

### 1. 健康检查

```bash
# 运行简化版健康检查（推荐）
./scripts/health-check-simple.sh

# 或使用完整版（需要bash 4+）
./scripts/health-check.sh
```

**输出示例**:
```
GroveMQ Cluster Health Check
=============================

1. Process Check
✓ All 3 nodes running

2. Port Check
✓ Port 1883 OK
✓ Port 1884 OK
✓ Port 1885 OK

3. Metrics Check
  Node port 8082: 0 connections
  Node port 8084: 0 connections
  Node port 8086: 0 connections

4. Connectivity Test
✓ Cross-node messaging OK
```

### 2. 性能测试

```bash
# 运行完整性能测试套件
./scripts/performance-test.sh
```

**测试项目**:
- 延迟测试 (1000次迭代)
- 吞吐量测试 (60秒)
- 并发连接测试 (10-100连接)
- QoS级别对比
- 压力测试 (120秒)

**测试结果**: 保存在 `perf-results-YYYYMMDD-HHMMSS/` 目录

---

## 📊 Grafana仪表板

### 安装步骤

1. **安装Grafana**:
```bash
# macOS
brew install grafana

# Ubuntu/Debian
sudo apt-get install grafana

# 启动服务
brew services start grafana  # macOS
sudo systemctl start grafana # Linux
```

2. **导入仪表板**:
   - 访问 http://localhost:3000 (默认用户名/密码: admin/admin)
   - 导航到 Dashboard → Import
   - 上传 `monitoring/grafana-dashboard.json`

### 仪表板内容

| 面板 | 描述 |
|------|------|
| 活跃连接数 | 各节点当前连接数时间序列 |
| 总连接数 | 整个集群的总连接数（仪表盘） |
| 消息吞吐量 | 发送/接收消息速率 |
| 会话和订阅 | 活跃会话和订阅数量 |
| 包速率 | 发送/接收包的速率 |
| 丢弃消息率 | 消息丢失率（带告警阈值） |
| 节点运行时间 | 各节点的运行时间 |
| 生命周期总连接数 | 历史总连接数 |
| 总丢弃消息数 | 历史总丢弃消息数 |
| 节点状态 | 各节点的在线/离线状态 |

---

## 🔔 Prometheus告警

### 配置Prometheus

1. **创建Prometheus配置** (`prometheus.yml`):
```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

# 加载告警规则
rule_files:
  - "prometheus-alerts.yml"

# 抓取配置
scrape_configs:
  - job_name: 'grovemq'
    static_configs:
      - targets:
        - 'localhost:8082'  # node1
        - 'localhost:8084'  # node2
        - 'localhost:8086'  # node3
```

2. **启动Prometheus**:
```bash
prometheus --config.file=prometheus.yml
```

3. **访问Web界面**: http://localhost:9090

### 告警规则详情

#### 节点可用性告警

| 告警名称 | 触发条件 | 严重性 |
|---------|----------|--------|
| `GrovemqNodeDown` | 节点宕机超过1分钟 | Critical |
| `GrovemqClusterDegraded` | 少于3个节点运行超过2分钟 | Warning |
| `GrovemqNodeRestarted` | 节点运行时间少于5分钟 | Info |

#### 连接告警

| 告警名称 | 触发条件 | 严重性 |
|---------|----------|--------|
| `HighConnectionCount` | 连接数超过1000持续5分钟 | Warning |
| `ConnectionSpike` | 新连接速率超过100/秒持续2分钟 | Warning |

#### 消息告警

| 告警名称 | 触发条件 | 严重性 |
|---------|----------|--------|
| `HighMessageDropRate` | 丢弃速率超过10消息/秒持续2分钟 | Warning |
| `MessageDropRateCritical` | 丢弃速率超过100消息/秒持续1分钟 | Critical |

#### 会话告警

| 告警名称 | 触发条件 | 严重性 |
|---------|----------|--------|
| `HighSessionCount` | 会话数超过5000持续5分钟 | Warning |

#### 性能告警

| 告警名称 | 触发条件 | 严重性 |
|---------|----------|--------|
| `PacketProcessingLag` | 接收与发送速率差超过1000包/秒持续5分钟 | Warning |

---

## 🔧 配置AlertManager

1. **创建AlertManager配置** (`alertmanager.yml`):
```yaml
global:
  resolve_timeout: 5m

route:
  group_by: ['alertname', 'cluster']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 12h
  receiver: 'default'

receivers:
  - name: 'default'
    webhook_configs:
      - url: 'http://localhost:5001/'  # 你的Webhook URL
```

2. **启动AlertManager**:
```bash
alertmanager --config.file=alertmanager.yml
```

---

## 📈 性能测试详解

### 测试1: 延迟测试

**目的**: 测量消息端到端延迟

**方法**:
- 发送1000条消息
- 记录每条消息的往返时间
- 计算统计数据（平均值、最小值、最大值、P50/P95/P99）

**示例结果**:
```
Latency Test Results
====================
Iterations: 1000
Average: 1.2ms
Minimum: 0.8ms
Maximum: 5.3ms
P50: 1.0ms
P95: 2.1ms
P99: 3.5ms
```

### 测试2: 吞吐量测试

**目的**: 测量最大消息吞吐量

**方法**:
- 持续发送消息60秒
- 计算平均吞吐量 (msg/s)

**示例结果**:
```
Throughput Test Results
=======================
Duration: 60s
Total Messages: 45000
Average Throughput: 750.00 msg/s
```

### 测试3: 并发连接测试

**目的**: 测试系统在高并发下的表现

**方法**:
- 以10为步长，从10到100个并发连接
- 测量完成时间和平均每连接时间

**示例结果**:
```
Connections: 10 | Total Time: 250ms | Avg per conn: 25.00ms
Connections: 20 | Total Time: 480ms | Avg per conn: 24.00ms
Connections: 50 | Total Time: 1200ms | Avg per conn: 24.00ms
Connections: 100 | Total Time: 2500ms | Avg per conn: 25.00ms
```

### 测试4: 压力测试

**目的**: 评估系统在高负载下的稳定性

**方法**:
- 以目标速率发送消息120秒
- 记录成功率和实际吞吐量

**示例结果**:
```
Stress Test Results
===================
Duration: 120s
Target Rate: 1000 msg/s

Results:
--------
Total Messages: 115000
Failed Messages: 250
Success Rate: 99.78%
Actual Throughput: 958.33 msg/s
```

---

## 🛠️ 工具使用指南

### 健康检查脚本

**使用场景**:
- 部署后验证
- 定期监控检查
- 故障排查

**检查项目**:
1. 进程检查 - 验证3个节点都在运行
2. 端口检查 - 验证所有关键端口正常监听
3. 指标检查 - 获取Prometheus指标并显示关键数据
4. 连接测试 - 执行跨节点消息测试

**退出码**:
- `0`: 所有检查通过
- `1`: 发现警告
- `2`: 发现严重问题

### 性能测试脚本

**使用场景**:
- 基线性能评估
- 性能回归测试
- 容量规划

**前置条件**:
- 集群正在运行
- `bc` 计算器已安装
- `bin/cluster-test` 已构建

**输出**:
- 原始数据: `perf-results-*/metrics/*.txt`
- 报告: `perf-results-*/reports/*.txt`
- 总结: `perf-results-*/PERFORMANCE_REPORT.md`

---

## 🔍 故障排查

### 问题1: Grafana连接不上Prometheus

**症状**: Grafana仪表板显示"No data"

**解决方案**:
1. 检查Prometheus是否运行: `curl http://localhost:9090`
2. 在Grafana中配置数据源:
   - Settings → Data Sources → Add data source
   - 选择Prometheus
   - URL: `http://localhost:9090`
   - 点击 "Save & Test"

### 问题2: 告警没有触发

**症状**: 即使条件满足，也不发送告警

**解决方案**:
1. 检查Prometheus规则是否加载:
   ```bash
   curl http://localhost:9090/api/v1/rules
   ```
2. 检查告警状态:
   - 访问 http://localhost:9090/alerts
3. 验证AlertManager配置:
   ```bash
   curl http://localhost:9093/api/v1/alerts
   ```

### 问题3: 性能测试失败

**症状**: `performance-test.sh` 报错或结果异常

**解决方案**:
1. 确保集群正在运行: `pgrep -f grovemq`
2. 检查bc是否安装: `which bc`
3. 验证cluster-test可用: `./bin/cluster-test`
4. 查看详细日志: `./logs/*.log`

---

## 📚 参考文档

- [Prometheus官方文档](https://prometheus.io/docs/)
- [Grafana官方文档](https://grafana.com/docs/)
- [AlertManager文档](https://prometheus.io/docs/alerting/latest/alertmanager/)
- [GroveMQ完整文档](../README.md)
- 混沌测试文档

---

## 🤝 贡献

欢迎改进监控工具和仪表板！请提交PR或Issue。

---

**最后更新**: 2025-10-11
**维护者**: GroveMQ团队
