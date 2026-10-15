# SQL 方言转换

Data Fabric 提供独立的 SQL 转换接口，并允许查询请求携带 sourceDialect，在执行前转换到目标数据源所用的方言。

## 支持范围

转换器声明支持 MySQL、PostgreSQL、Trino、Spark、Oracle、SQL Server、Snowflake、SQLite、DuckDB 和 Redshift。部分兼容数据库类型会映射到这些方言。未映射或不支持的数据库类型会返回转换错误。

## 使用条件

方言转换调用 Python 3 脚本，并依赖 sqlglot：

~~~bash
python3 -m pip install -r requirements-sql-translation.txt
~~~

当前 Go 代码从仓库根目录相对路径调用该脚本。未使用转换功能时，HTTP 服务和 MCP 的基础查询路径不需要安装 Python 包。

## HTTP 接口

- POST /api/v1/sql/translate：转换 SQL。
- GET /api/v1/sql/dialects：返回声明支持的方言。
- POST /api/v1/sql/validate：验证 SQL 请求。

查询 API 可选传入 sourceDialect。转换器结果仍需在目标数据库上验证；不同数据库的函数、类型、排序规则及边界行为可能不同，转换成功不保证语义完全一致。
