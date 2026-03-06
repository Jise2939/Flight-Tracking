# AviationStack API 航班数据爬虫

使用 AviationStack API 获取航班取消/延误、历史趋势、绕飞时间、运力对比等数据。

## 功能特性

- **航班取消/延误统计** - 统计指定机场的航班取消和延误数量
- **受影响旅客估算** - 基于机型容量估算受影响旅客人数
- **历史趋势分析** - 支持过去3个月的历史数据查询，分析变化趋势
- **绕飞时间检测** - 对比计划与实际飞行时间，检测绕飞情况
- **运力对比分析** - 统计每周航班班次和运营航空公司数量

## 文件说明

| 文件名 | 说明 |
|--------|------|
| `aviationstack_crawler.py` | 主爬虫脚本 |
| `config.yaml` | 配置文件（机场列表、查询设置） |
| `.env` | 环境变量（API Key，已受gitignore保护） |
| `.env.example` | 环境变量模板 |
| `requirements.txt` | Python依赖 |
| `run_crawler.sh` | 运行脚本 |
| `setup.sh` | 安装脚本 |
| `test_aviationstack.py` | 功能测试 |
| `verify_setup.py` | 配置验证 |

## 快速开始

### 1. 获取 API Key

访问 https://aviationstack.com/ 注册并获取免费 API Key

### 2. 安装依赖

```bash
./setup.sh
```

或手动安装：

```bash
pip install -r requirements.txt
```

### 3. 配置 API Key

编辑 `.env` 文件，替换 API Key：

```bash
AVIATIONSTACK_API_KEY=your_api_key_here
```

### 4. 运行爬虫

```bash
./run_crawler.sh
```

或直接运行 Python：

```bash
python3 aviationstack_crawler.py
```

## 配置说明

### 机场列表

在 `config.yaml` 中配置要分析的机场：

```yaml
departure_airports:
  hong_kong:
    - HKG  # 香港
  
  china_mainland:
    - PEK  # 北京
    - PVG  # 上海
    # ... 更多机场

middle_east_airports:
  - DXB  # 迪拜
  - DOH  # 多哈
  # ... 更多机场
```

### 查询设置

```yaml
query:
  limit: 100         # 每次请求最大记录数
  batch_size: 10     # 并发请求数量
  delay: 1.0         # 请求间隔（秒）
```

## 输出格式

数据保存在 `output/` 目录：

- `aviationstack_cancellations_YYYYMMDD_HHMMSS.json` - 取消/延误数据
- `aviationstack_duration_changes_YYYYMMDD_HHMMSS.json` - 飞行时间变化
- `aviationstack_capacity_YYYYMMDD_HHMMSS.json` - 运力数据
- `aviationstack_summary_YYYYMMDD_HHMMSS.json` - 汇总报告

### 数据字段

**取消/延误数据：**
```json
{
  "airport": "DXB",
  "cancelled_flights": 45,
  "delayed_flights": 128,
  "affected_passengers": 16425,
  "daily_breakdown": {
    "2026-03-01": {"cancelled": 10, "delayed": 25, "affected": 3650}
  },
  "trend": {
    "cancelled_change": 15,
    "cancelled_change_percent": 50.0
  }
}
```

**飞行时间变化：**
```json
{
  "route": "PEK-LHR",
  "avg_scheduled_duration_minutes": 620,
  "avg_actual_duration_minutes": 685,
  "time_increase_minutes": 65,
  "time_increase_percent": 10.48
}
```

## 注意事项

1. **API 限制**：免费版每月 100 次请求，每次最多 100 条记录
2. **历史数据**：支持过去 3 个月的数据
3. **速率限制**：建议设置请求间隔避免触发限制

## GitHub Actions

配置 Secret：
- `AVIATIONSTACK_API_KEY` - 你的 AviationStack API Key

工作流会自动每天运行并将结果上传为 artifacts。

## 支持

如需帮助，请查看：
- AviationStack API 文档: https://aviationstack.com/documentation