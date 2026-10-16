# Data Fabric MCP

Data Fabric 提供可选的 MCP stdio 服务，让受信任的 MCP 客户端通过统一查询服务访问已配置的数据源。HTTP 和 SSE 传输尚未实现。

详细配置、工具说明和安全边界见 [MCP 使用说明](MCP_DOCUMENTATION.md)。MCP 运行架构见 [MCP 架构](MCP_ARCHITECTURE.md)。

MCP 默认关闭。HTTP 与 MCP 服务启动前均需要通过 DATA_FABRIC_CREDENTIAL_ACTIVE_KEY_ID 和 DATA_FABRIC_CREDENTIAL_KEYS 注入数据源凭据加密密钥。密钥生成和轮换说明见配置指南。启用方法：

~~~yaml
mcp:
  enabled: true
  transport: "stdio"
~~~

然后从仓库根目录运行：

~~~bash
go run ./cmd/server
~~~
