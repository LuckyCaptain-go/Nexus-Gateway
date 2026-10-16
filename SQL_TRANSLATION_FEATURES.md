# 统一 SQL v1 范围与兼容矩阵

Data Fabric 的查询入口让调用方通过一个 API 提交只读 SQL，并把查询路由到已配置的数据源或跨源查询引擎。v1 先定义可验证的查询子集，不承诺完整 SQL 标准兼容，也不承诺不同数据库之间语义完全相同。

## 请求约定

- `sourceDialect` 省略时，SQL 按目标数据源或查询引擎的原生方言解释，Data Fabric 不做方言转换。
- 提供 `sourceDialect` 时，Data Fabric 尝试将输入 SQL 转换为目标方言；转换成功只表示语法树能够转换，不代表目标引擎一定接受或语义完全等价。
- 查询 API 只允许单条只读查询。当前验证器接受 `SELECT` 和 `UNION` 查询，拒绝 DDL、DML、过程调用、多语句和 SQL 注释。
- 对 Trino、Presto 和 OpenLooKeng 类型的数据源，`catalog.schema.table` 可用于定位协调器已配置的目录。目录权限、连接器能力和跨目录执行能力由对应引擎配置决定。

## v1 兼容矩阵

| 查询构造 | v1 状态 | 当前边界与验证方式 |
| --- | --- | --- |
| `SELECT` 投影、列别名、常量和基本表达式 | 纳入范围 | 网关只读校验；翻译样例由 #165 回归测试固定。目标引擎执行尚需 #164 集成验证。 |
| `WHERE` 过滤与布尔条件 | 纳入范围 | 网关进行语句解析和安全检查；具体函数、类型转换及空值规则依目标引擎。 |
| `INNER JOIN`、`LEFT JOIN` 等关系连接 | 纳入范围 | SQL 结构可翻译；跨目录连接还要求协调器及相关连接器支持。 |
| `GROUP BY`、聚合函数和 `HAVING` | 纳入范围 | 仅对回归样例保证翻译输出；聚合精度、溢出和类型规则依引擎。 |
| 常见标量函数与 DATE/TIMESTAMP 字面量 | 样例覆盖 | 回归样例验证转换后的 SQL；不同引擎的类型推导、时区和函数边界仍依引擎。 |
| `ORDER BY` | 纳入范围 | 排序规则、大小写和 NULL 排序顺序依引擎，不保证跨引擎结果顺序完全一致。 |
| `LIMIT` 与 `OFFSET` | 纳入范围 | 翻译器会按目标方言生成分页语法；大结果集分页仍受目标引擎行为影响。 |
| `catalog.schema.table` 跨目录引用 | 查询引擎原生 SQL | 适用于协调器已配置的目录；传入 `sourceDialect` 并不能自动创建目录或修复权限。 |
| CTE、窗口函数、引擎专有函数、复杂类型和时间旅行语法 | 非统一保证 | 可尝试使用目标引擎原生 SQL；方言转换或执行结果必须单独验证。 |
| DDL、DML、存储过程调用、多语句和注释 | 拒绝 | 网关验证阶段返回只读或安全校验错误，不发送给数据源。 |

### 方言与引擎状态

- Trino 是当前跨目录查询路径的首个目标方言；查询能力仍取决于协调器版本和已安装连接器。
- Presto 与 OpenLooKeng 可通过兼容的查询引擎接入路径使用。当前转换层将 OpenLooKeng 语法按 Presto 方言处理；引擎特有语法和执行兼容性尚未由真实协调器端到端验证。
- 其他 SQL 方言可以作为转换输入，但只有回归矩阵明确覆盖的语法才算 Data Fabric v1 的转换承诺。转换器能识别一种方言，不等于产品已验证该方言的全部语法。

## 失败行为

1. 输入 SQL 无法按声明的 `sourceDialect` 解析、方言不受支持，或目标方言无法表达输入中的构造时，转换请求返回明确错误；查询 API 不会悄悄退回执行原始 SQL。
2. 未声明 `sourceDialect` 时，Data Fabric 将 SQL 原样交给目标引擎。目标引擎拒绝语法或权限时，查询 API 返回执行错误。
3. 转换器无法判断数据内容、表结构、时区、排序规则、精度或连接器特性。因此，即使转换成功，也可能需要调用方根据目标引擎调整 SQL，并通过真实引擎验证结果语义。

## 示例

省略 `sourceDialect`，直接使用查询引擎 SQL 做跨目录查询：

```sql
SELECT o.order_id, c.customer_name
FROM iceberg.sales.orders AS o
JOIN postgresql.crm.customers AS c
  ON o.customer_id = c.customer_id
WHERE o.status = 'open'
ORDER BY o.created_at DESC
LIMIT 100
```

如果输入采用其他方言，查询请求可以提供 `sourceDialect`，让 Data Fabric 尝试转换为目标引擎方言。转换能力以 #165 的回归样例为准；跨目录真实执行验证由 #164 跟进。
