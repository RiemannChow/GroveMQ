# GroveMQ 官网（GitHub Pages 静态站点）

这是 GroveMQ 的产品官网静态站点，4 个页面：

- `index.html` — 首页：能力概览、数据指标、A2A 亮点、架构预览
- `architecture.html` — 整体架构：五层架构、模块地图、集群拓扑、端口与运维
- `agent.html` — Agent 能力：A2A over MQTT 主题契约、时序、状态、安全、A2A 1.0 兼容
- `get-started.html` — 接入指南：本地构建、部署、启用 A2A、发布 Agent Card、接入方式

样式与交互在 `assets/`，纯静态，无构建步骤，无需后端。

## 本地预览

```sh
cd grovemq-site
python3 -m http.server 8899
# 浏览器打开 http://localhost:8899
```

## 发布到 GitHub Pages

### 方式 A：独立仓库（推荐，便于绑定自定义域名）

1. 新建一个公开仓库，例如 `RiemannChow/grovemq-website`。
2. 把本目录的全部内容（含 `.nojekyll`）提交到该仓库的 `main` 分支根目录。
3. 仓库 → Settings → Pages → Build and deployment：
   - Source 选 `Deploy from a branch`
   - Branch 选 `main` / `/ (root)` → Save
4. 等待部署完成，得到默认地址 `https://<user>.github.io/grovemq-website/`。

### 方式 B：放在现有 GroveMQ 仓库的 `gh-pages` 分支

1. 在 GroveMQ 仓库创建 `gh-pages` 分支，内容为本目录文件。
2. Settings → Pages → Source 选 `gh-pages` / `/ (root)` → Save。

> `.nojekyll` 已包含，用于避免 Jekyll 忽略以点开头的文件；保留它即可。

## 自定义域名映射

在 GitHub 仓库 Settings → Pages → Custom domain 填入你的域名（例如 `grovemq.example.com`）并保存，GitHub 会自动为该仓库创建 `CNAME` 文件并配置证书（勾选 Enforce HTTPS）。

然后在你的 DNS 服务商添加解析：

- **绑定子域名**（如 `grovemq.example.com`）：添加 `CNAME` 记录
  ```
  grovemq.example.com  CNAME  <user>.github.io.
  ```
- **绑定根域名**（如 `example.com`）：添加 `A` 记录（`192.30.252.153`、`192.30.252.154`）加上 `CNAME` 记录 `www → <user>.github.io.`（GitHub 会自动处理根域与 www 的跳转）。

DNS 生效（通常几分钟到几小时）后，GitHub 会自动签发 HTTPS 证书。

> 注意：如果你打算把域名映射到**用户站点** `<user>.github.io`（仓库名就是 `<user>.github.io`），则 CNAME 目标同样是 `<user>.github.io.`，但根目录就是站点根，路径无需带仓库名。

## 内容维护

- 页面内容直接编辑对应 HTML 文件；公共样式在 `assets/style.css`，交互在 `assets/main.js`。
- 所有内链使用相对路径（`index.html`、`agent.html` 等），部署在根路径或项目路径下均可正常工作。
- 站点中的技术事实来自 GroveMQ 仓库的 `README.md`、`config.yaml`、`docs/guides/DEPLOYMENT.md` 与 `docs/design/A2A_OVER_MQTT_SPEC.md`，更新产品能力时请同步核对。
