#!/usr/bin/env python3
"""
CSV合并工具使用示例
"""

from csv_merger import CSVMerger
from pathlib import Path


def example_1_basic_merge():
    """示例1: 基本合并"""
    print("=" * 50)
    print("示例1: 基本合并")
    print("=" * 50)
    
    merger = CSVMerger(
        input_dir='./data/csv_files',
        output_file='./output/merged.csv'
    )
    
    files_count, rows_count = merger.merge()
    print(f"\n结果: 合并了 {files_count} 个文件，总共 {rows_count:,} 行")


def example_2_custom_delimiter():
    """示例2: 自定义分隔符"""
    print("\n" + "=" * 50)
    print("示例2: 处理分号分隔的文件")
    print("=" * 50)
    
    merger = CSVMerger(
        input_dir='./data/csv_files_semicolon',
        output_file='./output/merged_semicolon.csv',
        delimiter=';'
    )
    
    files_count, rows_count = merger.merge()
    print(f"\n结果: 合并了 {files_count} 个文件，总共 {rows_count:,} 行")


def example_3_large_files():
    """示例3: 处理大型文件"""
    print("\n" + "=" * 50)
    print("示例3: 处理超大型文件(10G+)")
    print("=" * 50)
    
    merger = CSVMerger(
        input_dir='/large_data/csv_files',
        output_file='/output/merged_large.csv',
        chunk_size=500000  # 每次处理50万行
    )
    
    files_count, rows_count = merger.merge()
    summary = merger.get_summary()
    
    print(f"\n合并摘要:")
    print(f"  文件数: {summary['total_files']}")
    print(f"  行数: {summary['total_rows']:,}")
    print(f"  输出大小: {summary['output_size']}")


def example_4_with_pattern():
    """示例4: 使用文件模式过滤"""
    print("\n" + "=" * 50)
    print("示例4: 只合并特定模式的文件")
    print("=" * 50)
    
    merger = CSVMerger(
        input_dir='./data/mixed_files',
        output_file='./output/merged_filtered.csv',
        pattern='data_*.csv'  # 只合并 data_ 开头的文件
    )
    
    files_count, rows_count = merger.merge()
    print(f"\n结果: 合并了 {files_count} 个文件，总共 {rows_count:,} 行")


if __name__ == '__main__':
    # 根据需要运行相应的示例
    
    # 基本合并
    # example_1_basic_merge()
    
    # 自定义分隔符
    # example_2_custom_delimiter()
    
    # 处理超大型文件
    # example_3_large_files()
    
    # 文件模式过滤
    # example_4_with_pattern()
    
    print("请取消注释要运行的示例...")