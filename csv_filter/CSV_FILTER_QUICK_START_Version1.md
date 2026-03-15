# CSV Filter 快速开始指南

## 您的需求

删除10G+ CSV文件中第一列小于0的行数据，生成新的CSV文件。

## 解决方案

### 方法1: 命令行（推荐）

**最简单的命令：**

```bash
python csv_filter.py your_input_file.csv output_file.csv --less-than 0