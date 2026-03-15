# CSV文件合并工具

一个高性能的Python工具，用于合并目录下的多个CSV文件。特别设计用于处理超大型文件（10G+），采用流式处理避免内存溢出。

## ✨ 特性

- **流式处理**: 使用迭代器逐行读写，不加载整个文件到内存
- **超大文件支持**: 可处理单个文件几十GB的场景
- **编码自动检测**: 自动识别文件编码（UTF-8, GBK, Latin-1等）
- **进度显示**: 实时显示处理进度和统计信息
- **灵活配置**: 支持自定义分隔符、文件模式、处理块大小
- **错误处理**: 完善的错误捕获和日志记录
- **列数验证**: 自动检查所有文件列数是否一致

## 📋 系统要求

- Python 3.7+
- chardet（用于编码检测）

## 🚀 安装

```bash
# 安装依赖
pip install chardet
```

## 💡 使用方法

### 命令行使用

```bash
# 基本用法
python csv_merger.py /path/to/csv/directory -o output.csv

# 自定义分隔符
python csv_merger.py /path/to/csv/directory -o output.csv --delimiter ";"

# 指定文件模式
python csv_merger.py /path/to/csv/directory -o output.csv --pattern "data_*.csv"

# 处理大型文件（增加chunk_size）
python csv_merger.py /path/to/csv/directory -o output.csv --chunk-size 500000

# 指定编码
python csv_merger.py /path/to/csv/directory -o output.csv --encoding utf-8
```

### Python API 使用

```python
from csv_merger import CSVMerger

# 创建合并器实例
merger = CSVMerger(
    input_dir='./data',
    output_file='./merged.csv',
    delimiter=',',
    chunk_size=100000,
    encoding='utf-8'
)

# 执行合并
files_count, rows_count = merger.merge()

# 获取摘要信息
summary = merger.get_summary()
print(summary)
# 输出:
# {
#     'total_files': 5,
#     'total_rows': 5000000,
#     'output_file': './merged.csv',
#     'output_size': '2.50GB',
#     'header_columns': 10
# }
```

## 📊 工作流程

1. **扫描目录**: 查找所有匹配的CSV文件（按名称排序）
2. **读取第一个文件**: 
   - 将第一个文件的第一行作为表头
   - 验证所有后续文件的列数一致性
3. **逐行合并**: 
   - 将第一个文件的所有行写入输出文件
   - 将后续文件的数据行（跳过表头）追加到输出文件
4. **输出结果**: 生成合并后的CSV文件

## 🔧 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `input_dir` | 输入目录路径 | 必需 |
| `output_file` | 输出CSV文件路径 | 必需 |
| `pattern` | 文件匹配模式 | `*.csv` |
| `delimiter` | CSV分隔符 | `,` |
| `chunk_size` | 每次处理的行数 | `100000` |
| `encoding` | 文件编码 | 自动检测 |

## 📈 性能优化建议

### 1. 调整chunk_size

对于大文件，可以增加chunk_size以提高性能：

```python
merger = CSVMerger(
    input_dir='./data',
    output_file='./merged.csv',
    chunk_size=500000  # 从默认的100000增加到500000
)
```

### 2. 并行处理多个合并任务

如果有多个独立的合并任务，可以使用多进程：

```python
from multiprocessing import Pool
from csv_merger import CSVMerger

def merge_task(config):
    merger = CSVMerger(**config)
    return merger.merge()

configs = [
    {'input_dir': './data1', 'output_file': './output1.csv'},
    {'input_dir': './data2', 'output_file': './output2.csv'},
    {'input_dir': './data3', 'output_file': './output3.csv'},
]

with Pool(3) as pool:
    results = pool.map(merge_task, configs)
```

## 📝 日志

日志输出到两个地方：
- 控制台：实时进度信息
- `csv_merger.log`：完整的操作日志

```
2026-03-15 14:30:45,123 - INFO - 输入目录: /data/csv_files
2026-03-15 14:30:45,124 - INFO - 找到 5 个CSV文件
2026-03-15 14:30:50,456 - INFO - 处理文件 [1/5]: part1.csv
2026-03-15 14:30:55,789 - INFO -   已处理 100,000 行 | 总计 100,000 行
...
2026-03-15 15:15:30,123 - INFO - ✓ 合并完成!
```

## ⚠️ 注意事项

1. **列数一致性**: 工具会验证所有CSV文件的列数必须一致。如果列数不同，会报错并停止合并。

2. **文件编码**: 工具自动检测文件编码。如果自动检测有问题，可以使用 `--encoding` 参数指定。

3. **输出文件路径**: 确保输出文件所在目录存在或有写入权限。

4. **文件顺序**: 文件按名称字母顺序合并。如果需要特定顺序，请对源文件进行命名。

5. **内存占用**: 工具不会一次性加载文件，但操作系统缓冲可能占用一些内存（通常<100MB）。

## 🐛 故障排除

### 问题1: "列数不匹配"错误

**原因**: CSV文件的列数不一致

**解决**:
```bash
# 检查每个CSV文件的列数
for file in *.csv; do
  echo "$file: $(head -1 $file | tr ',' '\n' | wc -l) 列"
done
```

### 问题2: 编码错误

**症状**: 看到乱码或编码错误

**解决**:
```bash
python csv_merger.py ./data -o output.csv --encoding gbk
# 或指定其他编码: utf-8, latin-1, cp1252等
```

### 问题3: 处理速度慢

**症状**: 处理进度显示很慢

**解决**:
```bash
# 增加chunk_size
python csv_merger.py ./data -o output.csv --chunk-size 500000
```

## 📄 许可证

MIT License

## 🤝 贡献

欢迎提交问题和改进建议！

---

**最后更新**: 2026-03-15