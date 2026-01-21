# Nexus-Gateway MCP Server - 数据库连接与查询服务

<p>Nexus-Gateway MCP Server是一款基于先进数据库连接与AI技术构建的高效工具，通过标准化接口，可无缝集成至各类AI应用系统。它专为解决从多样化数据库中精准获取关键数据的难题而设计，能让AI模型快速获取所需信息，支持通过统一网关连接多种数据库类型（MySQL、PostgreSQL、Oracle、SQL Server等）并执行查询，提升数据分析效率与知识挖掘能力。</p>

## 核心功能

<ol>
<li><strong>多数据库类型支持</strong>：插件具备强大兼容性，支持MySQL、PostgreSQL、Oracle、SQL Server、MariaDB、ClickHouse、Doris、StarRocks、Druid、OceanBase、TiDB、GaussDB、达梦、人大金仓、MongoDB等多种数据库格式。无论是关系型数据库、数据仓库，还是国产化数据库，都能准确连接和查询。</li>
<li><strong>精准数据提取</strong>：运用先进的SQL解析与执行技术，而非使用静态数据模型。插件可精准提取数据库中的结构化信息，包括表结构、字段类型、数据内容、统计信息等。例如在一份业务数据MySQL数据库中，能快速抽取出用户信息、订单数据、交易记录等关键业务字段；在企业ERP系统里，精准定位并提取客户信息、产品库存、销售业绩等重要内容。</li>
<li><strong>结构化数据输出</strong>：将查询的数据库内容转化为结构化数据格式，如JSON、XML。以JSON为例，会按照预先设定的数据模型，将查询结果组织成键值对形式，方便AI系统进一步分析、存储与调用。例如将一张用户信息表转化为包含ID、姓名、邮箱、注册时间等键值对的JSON数据，清晰呈现数据结构。</li>
<li><strong>自定义查询规则</strong>：提供灵活的自定义配置功能，AI模型可依据自身需求，制定特定的SQL查询规则。如在电商领域，可自定义查询提取商品名称、销量、价格、评价等信息；在金融行业，对交易数据库设定规则提取客户基本信息、交易记录、风险等级等关键内容。</li>
</ol>

## 使用场景

<ol>
<li><strong>金融行业</strong>
<ol>
<li>信贷审核：从数据库中提取用户身份信息、财务数据及交易记录，加速风控评估。</li>
<li>风险监控：从交易数据库中抽取异常交易记录、风险指标，提升监控效率。</li>
</ol>
</li>
<li><strong>医疗领域</strong>
<ol>
<li>电子病历：自动提取患者基本信息、病史、诊断结果，助力结构化管理与诊疗参考。</li>
<li>医保审核：从医疗数据库中抽取费用明细与病历病情数据，辅助审核合规性。</li>
</ol>
</li>
<li><strong>企业办公</strong>
<ol>
<li>客户管理：抽取CRM数据库中的客户信息、联系方式、交易历史，优化客户关系管理。</li>
<li>财务分析：从财务数据库中提取资产、利润等核心指标，支持经营决策数据整理。</li>
</ol>
</li>
<li><strong>其他场景</strong>
<ol>
<li>科研数据：抽取实验数据库中的实验结果、参数配置，辅助学术管理与研究。</li>
<li>政务数据：提取政务服务数据库中的办事记录、审批信息，提升政务服务效率。</li>
</ol>
</li>
</ol>

## MCP-数据库连接与查询服务的关键特性

<ol>
<li><strong>精准高效</strong>：采用先进数据库连接算法，支持多类型数据库，快速精准执行SQL查询，数据提取准确率高。</li>
<li><strong>便捷易用</strong>：标准化MCP接口，搭配详细文档，适配主流AI大模型，集成简单，开发周期短。</li>
<li><strong>灵活定制</strong>：支持自定义SQL查询规则，可按业务需求调整，适配多行业复杂场景。</li>
<li><strong>稳定安全</strong>：分布式架构保障7×24小时稳定运行，严格数据安全机制，确保信息安全。</li>
<li><strong>智能处理</strong>：自动将非结构化数据库内容转为结构化格式，便于AI分析、存储与调用。</li>
</ol>

<h2 id="支持类型">支持类型</h2>
<p>该MCP支持<strong>stdio</strong>和<strong>HTTP</strong>两种协议。</p>

<h2 id="常见问题解答">常见问题解答</h2>

<p><strong>1. BigModel上哪些类型的模型支持MCP？</strong></p>
<p>实际上，MCP是基于Function Calling接口来实现功能的，所以，使用MCP所选用的模型必须具备支持Function Calling的特性。Bigmodel上现在所有的语言模型（包括GLM-4-Plus、GLM-4-Flash等）均支持Function Calling。Z1系列推理模型因为不支持Function Calling，无法调用MCP。</p>

<p><strong>2.如何获取配置信息进行调用？</strong></p>
<ul>
<li>前往Nexus-Gateway管理后台</li>
<li>配置数据库连接信息（主机、端口、用户名、密码等）</li>
<li>启动MCP Server模式</li>
<li>在支持MCP的AI客户端中配置服务器信息</li>
</ul>

<p><strong>3. 现在有哪些MCP是支持在主流AI客户端中使用的？</strong></p>
<p>所有支持MCP协议的客户端，目前包括Claude Desktop、Cursor、VSCode等，都可以使用Nexus-Gateway MCP Server，只需配置正确的服务器地址和认证信息。</p>

<h2 id="使用教程">使用教程</h2>
<p>支持运行MCP协议的客户端，如<a href="https://claude.ai" target="_blank" rel="noopener">Claude Desktop</a>、<a href="https://code.visualstudio.com/" target="_blank" rel="noopener">VSCode</a>、<a href="https://cursor.com" target="_blank" rel="noopener">Cursor</a>等中配置，在Nexus-Gateway管理后台获取服务器连接信息，并按照文档内容设置服务器命令。</p>

<h2 id="如何在mcp-server上使用nexus-gateway数据库查询服务">如何在MCP Server上使用Nexus-Gateway数据库查询服务？</h2>

<h4 id="1-配置nexus-gateway-mcp-server">1. 配置Nexus-Gateway MCP Server</h4>
<p>首先，确保Nexus-Gateway MCP Server正在运行：</p>
<pre><code class="hljs language-bash"><span class="hljs-comment"># 修改配置文件 configs/config.yaml</span>
mcp:
  enabled: <span class="hljs-literal">true</span>
  transport: <span class="hljs-string">"stdio"</span>  <span class="hljs-comment"># 或 "http"</span>
  port: <span class="hljs-string">"8090"</span>
  host: <span class="hljs-string">"0.0.0.0"</span>

<span class="hljs-comment"># 启动服务</span>
go run ./cmd/server/main.go
</code></pre>

<h4 id="2-配置数据库连接">2. 配置数据库连接</h4>
<p>在Nexus-Gateway中添加需要连接的数据库：</p>
<ul>
<li>数据源名称：自定义名称</li>
<li>类型：选择对应的数据库类型</li>
<li>连接信息：主机、端口、数据库名、用户名、密码</li>
<li>高级配置：SSL设置、连接池大小、超时时间等</li>
</ul>

<h4 id="3-在ai客户端中配置">3. 在AI客户端中配置</h4>
<p>在支持MCP的客户端（如Claude Desktop、VSCode等）中配置服务器信息：</p>
<ul>
<li>服务器地址：对应Nexus-Gateway MCP Server的地址</li>
<li>认证信息：根据配置文件中的安全设置进行认证</li>
</ul>

<h4 id="4-开始使用">4. 开始使用</h4>
<p>配置完成后，AI模型即可通过MCP协议调用Nexus-Gateway的数据库连接和查询功能。</p>

<h4 id="5-在ai模型中使用">5. 在AI模型中使用</h4>
<p>配置完成后，AI模型可以直接调用以下工具：</p>
<ul>
<li><strong>list_data_sources</strong>：列出所有已配置的数据源</li>
<li><strong>execute_sql_query</strong>：执行SQL查询</li>
<li><strong>get_data_source_info</strong>：获取数据源详细信息</li>
<li><strong>validate_sql_query</strong>：验证SQL查询</li>
<li><strong>list_tables</strong>：列出数据源中的所有表</li>
</ul>

<h3 id="在ai客户端中使用">在AI客户端中使用</h3>

<h4 id="在claude-desktop中使用">在Claude Desktop中使用</h4>
<ol>
<li>启动Claude Desktop</li>
<li>在设置中启用MCP功能</li>
<li>配置Nexus-Gateway MCP Server</li>
<li>开始对话，AI将能够访问您的数据库</li>
</ol>

<h4 id="在vscode中使用">在VSCode中使用</h4>
<ol>
<li>安装支持MCP的扩展</li>
<li>配置Nexus-Gateway MCP Server</li>
<li>在聊天窗口中使用数据库查询功能</li>
</ol>

<h2 id="价格">价格</h2>
<p>数据库连接与查询（query工具）：开源免费</p>

<h2 id="如何申请部署nexus-gateway-mcp服务">如何申请部署Nexus-Gateway MCP服务？</h2>
<p>您可以联系Nexus-Gateway开发团队，填写部署申请表单，我们将协助您部署专属的MCP服务。</p>