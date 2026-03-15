#!/usr/bin/env python3
"""
超大CSV文件分析工具 - 计算行数和前三列的数值范围
支持处理10G+的文件，流式处理避免内存溢出
"""

import os
import sys
import csv
import argparse
import logging
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
from datetime import datetime
from dataclasses import dataclass, asdict
import chardet

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('csv_analyzer.log')
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ColumnStats:
    """列统计数据"""
    column_name: str
    column_index: int
    total_rows: int  # 该列的总数据行数
    valid_rows: int  # 该列有效（可转换为数值）的行数
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    is_numeric: bool = False
    data_type: str = "unknown"  # int, float, mixed, string
    
    @property
    def valid_ratio(self) -> float:
        """有效数据比率"""
        if self.total_rows == 0:
            return 0.0
        return self.valid_rows / self.total_rows * 100
    
    def to_dict(self) -> dict:
        """转换为字典"""
        data = asdict(self)
        data['valid_ratio'] = f"{self.valid_ratio:.2f}%"
        return data


class CSVAnalyzer:
    """
    CSV文件分析器 - 流式处理超大文件
    """
    
    def __init__(
        self,
        file_path: str,
        delimiter: str = ",",
        encoding: Optional[str] = None,
        chunk_size: int = 100000,
        analyze_columns: int = 3
    ):
        """
        初始化CSV分析器
        
        Args:
            file_path: CSV文件路径
            delimiter: CSV分隔符
            encoding: 文件编码（None表示自动检测）
            chunk_size: 每次显示进度的行数
            analyze_columns: 要分析的列数（默认前3列）
        """
        self.file_path = Path(file_path)
        self.delimiter = delimiter
        self.encoding = encoding
        self.chunk_size = chunk_size
        self.analyze_columns = analyze_columns
        
        # 统计信息
        self.total_rows = 0  # 包括表头
        self.header_row = None
        self.columns_stats = {}  # Dict[int, ColumnStats]
        self.file_size = 0
        
        self._validate_file()
    
    def _validate_file(self) -> None:
        """验证文件"""
        if not self.file_path.exists():
            raise FileNotFoundError(f"文件不存在: {self.file_path}")
        
        if not self.file_path.is_file():
            raise ValueError(f"不是文件: {self.file_path}")
        
        self.file_size = self.file_path.stat().st_size
        logger.info(f"分析文件: {self.file_path}")
        logger.info(f"文件大小: {self._format_size(self.file_size)}")
    
    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """格式化文件大小"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f}PB"
    
    def _detect_encoding(self) -> str:
        """自动检测文件编码"""
        if self.encoding:
            return self.encoding
        
        try:
            with open(self.file_path, 'rb') as f:
                raw_data = f.read(100000)  # 读取前100KB
                result = chardet.detect(raw_data)
                encoding = result.get('encoding', 'utf-8')
                
                if encoding and encoding.lower() != 'ascii':
                    logger.info(f"检测到编码: {encoding}")
                    return encoding
        except Exception as e:
            logger.warning(f"编码检测失败: {e}")
        
        return 'utf-8'
    
    def _try_to_number(self, value: str) -> Optional[float]:
        """尝试将字符串转换为数字"""
        if not value or not value.strip():
            return None
        
        try:
            # 尝试转换为整数或浮点数
            if '.' in value:
                return float(value)
            else:
                return float(int(value))
        except (ValueError, TypeError):
            return None
    
    def _detect_data_type(self, col_idx: int, values: List[Optional[float]]) -> str:
        """检测数据类型"""
        if not values:
            return "unknown"
        
        valid_values = [v for v in values if v is not None]
        if not valid_values:
            return "string"
        
        # 检查是否全是整数
        is_all_int = all(v == int(v) for v in valid_values)
        
        if is_all_int:
            return "int"
        else:
            return "float"
    
    def analyze(self) -> Dict[str, Any]:
        """
        分析CSV文件
        
        Returns:
            包含分析结果的字典
        """
        encoding = self._detect_encoding()
        
        logger.info(f"开始分析...")
        start_time = datetime.now()
        
        try:
            with open(self.file_path, 'r', encoding=encoding, errors='replace') as f:
                reader = csv.reader(f, delimiter=self.delimiter)
                
                # 读取表头
                try:
                    header = next(reader)
                    self.header_row = header
                    self.total_rows = 1
                except StopIteration:
                    raise ValueError("CSV文件为空或无法读取")
                
                # 初始化列统计对象
                num_columns = len(header)
                for i in range(min(self.analyze_columns, num_columns)):
                    self.columns_stats[i] = ColumnStats(
                        column_name=header[i],
                        column_index=i,
                        total_rows=0,
                        valid_rows=0
                    )
                
                logger.info(f"表头: {header}")
                logger.info(f"分析前 {min(self.analyze_columns, num_columns)} 列")
                
                # 缓存用于检测数据类型
                sample_values = {i: [] for i in range(min(self.analyze_columns, num_columns))}
                
                # 逐行读取数据
                for row_num, row in enumerate(reader, start=2):
                    if not row:  # 跳过空行
                        continue
                    
                    self.total_rows += 1
                    
                    # 处理前三列
                    for col_idx in range(min(self.analyze_columns, len(row))):
                        if col_idx not in self.columns_stats:
                            continue
                        
                        cell_value = row[col_idx].strip()
                        stats = self.columns_stats[col_idx]
                        stats.total_rows += 1
                        
                        # 尝试转换为数字
                        num_value = self._try_to_number(cell_value)
                        
                        if num_value is not None:
                            stats.valid_rows += 1
                            stats.is_numeric = True
                            
                            # 更新最小值和最大值
                            if stats.min_value is None:
                                stats.min_value = num_value
                                stats.max_value = num_value
                            else:
                                if num_value < stats.min_value:
                                    stats.min_value = num_value
                                if num_value > stats.max_value:
                                    stats.max_value = num_value
                            
                            # 采样用于类型检测
                            if len(sample_values[col_idx]) < 1000:
                                sample_values[col_idx].append(num_value)
                    
                    # 每处理chunk_size行显示进度
                    if self.total_rows % self.chunk_size == 0:
                        progress_mb = self._format_size(
                            self.file_path.stat().st_size
                        )
                        logger.info(
                            f"已处理 {self.total_rows:,} 行 | "
                            f"文件大小: {progress_mb}"
                        )
            
            # 检测数据类型
            for col_idx, stats in self.columns_stats.items():
                if stats.is_numeric:
                    stats.data_type = self._detect_data_type(
                        col_idx,
                        sample_values[col_idx]
                    )
                else:
                    stats.data_type = "string"
            
            # 计算耗时
            elapsed_time = datetime.now() - start_time
            
            # 生成结果
            result = {
                'file_path': str(self.file_path),
                'file_size': self.file_size,
                'file_size_formatted': self._format_size(self.file_size),
                'encoding': encoding,
                'total_rows': self.total_rows,
                'data_rows': self.total_rows - 1,  # 不包括表头
                'total_columns': len(header),
                'header': header,
                'analyzed_columns': min(self.analyze_columns, len(header)),
                'columns_stats': [asdict(self.columns_stats[i]) for i in sorted(self.columns_stats.keys())],
                'columns_stats_detailed': {
                    i: self.columns_stats[i].to_dict() 
                    for i in sorted(self.columns_stats.keys())
                },
                'elapsed_time': str(elapsed_time),
                'analysis_time_seconds': elapsed_time.total_seconds(),
                'rows_per_second': (self.total_rows - 1) / elapsed_time.total_seconds() if elapsed_time.total_seconds() > 0 else 0
            }
            
            self._print_report(result)
            
            return result
            
        except Exception as e:
            logger.error(f"分析过程中出错: {e}")
            raise
    
    def _print_report(self, result: Dict[str, Any]) -> None:
        """打印分析报告"""
        print("\n" + "="*80)
        print("CSV 文件分析报告")
        print("="*80)
        
        print(f"\n📄 文件信息:")
        print(f"  文件路径: {result['file_path']}")
        print(f"  文件大小: {result['file_size_formatted']}")
        print(f"  文件编码: {result['encoding']}")
        print(f"  分析耗时: {result['elapsed_time']}")
        
        print(f"\n📊 数据统计:")
        print(f"  总行数: {result['total_rows']:,} (包括表头)")
        print(f"  数据行数: {result['data_rows']:,}")
        print(f"  总列数: {result['total_columns']}")
        print(f"  分析列数: {result['analyzed_columns']}")
        print(f"  平均处理速度: {result['rows_per_second']:,.0f} 行/秒")
        
        print(f"\n📋 表头信息:")
        print(f"  {', '.join(f'[{i}]{name}' for i, name in enumerate(result['header']))}")
        
        print(f"\n📈 前 {result['analyzed_columns']} 列的数值范围分析:")
        print("-" * 80)
        
        for col_stats in result['columns_stats_detailed'].values():
            col_name = col_stats['column_name']
            col_idx = col_stats['column_index']
            data_type = col_stats['data_type']
            valid_ratio = col_stats['valid_ratio']
            
            print(f"\n  列 {col_idx}: '{col_name}'")
            print(f"    数据类型: {data_type}")
            print(f"    有效数据: {col_stats['valid_rows']}/{col_stats['total_rows']} ({valid_ratio})")
            
            if col_stats['is_numeric']:
                min_val = col_stats['min_value']
                max_val = col_stats['max_value']
                range_val = max_val - min_val
                
                if data_type == 'int':
                    print(f"    最小值: {int(min_val):,}")
                    print(f"    最大值: {int(max_val):,}")
                    print(f"    范围: {int(range_val):,}")
                else:
                    print(f"    最小值: {min_val:.6g}")
                    print(f"    最大值: {max_val:.6g}")
                    print(f"    范围: {range_val:.6g}")
            else:
                print(f"    注: 数据类型为字符串，不支持数值范围分析")
        
        print("\n" + "="*80 + "\n")


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(
        description='超大CSV文件分析工具 - 计算行数和前三列的数值范围',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 基本分析
  python csv_analyzer.py data.csv
  
  # 指定分隔符
  python csv_analyzer.py data.csv --delimiter ";"
  
  # 指定编码
  python csv_analyzer.py data.csv --encoding utf-8
  
  # 分析前5列
  python csv_analyzer.py data.csv --analyze-columns 5
  
  # 大文件分析（调整进度显示频率）
  python csv_analyzer.py huge_file.csv --chunk-size 500000
        """
    )
    
    parser.add_argument(
        'file_path',
        help='CSV文件路径'
    )
    parser.add_argument(
        '--delimiter',
        default=',',
        help='CSV分隔符 (默认: ,)'
    )
    parser.add_argument(
        '--encoding',
        help='文件编码 (默认: 自动检测)'
    )
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=100000,
        help='显示进度的行数间隔 (默认: 100000)'
    )
    parser.add_argument(
        '--analyze-columns',
        type=int,
        default=3,
        help='分析的列数 (默认: 3)'
    )
    parser.add_argument(
        '--json',
        action='store_true',
        help='以JSON格式输出结果'
    )
    
    args = parser.parse_args()
    
    try:
        analyzer = CSVAnalyzer(
            file_path=args.file_path,
            delimiter=args.delimiter,
            encoding=args.encoding,
            chunk_size=args.chunk_size,
            analyze_columns=args.analyze_columns
        )
        
        result = analyzer.analyze()
        
        if args.json:
            import json
            # 将float转换为字符串以保持精度
            def convert_floats(obj):
                if isinstance(obj, float):
                    return f"{obj:.6g}"
                elif isinstance(obj, dict):
                    return {k: convert_floats(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_floats(v) for v in obj]
                return obj
            
            json_result = convert_floats(result)
            print(json.dumps(json_result, indent=2, ensure_ascii=False))
        
        return 0
        
    except Exception as e:
        logger.error(f"程序错误: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())