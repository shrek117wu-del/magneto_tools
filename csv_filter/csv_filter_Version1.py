#!/usr/bin/env python3
"""
高性能CSV行过滤工具
支持处理超大型CSV文件（10G+）
根据第一列的值删除不符合条件的行
"""

import os
import sys
import csv
import argparse
import logging
from pathlib import Path
from typing import Optional, Callable, Tuple
from datetime import datetime
import chardet

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('csv_filter.log')
    ]
)
logger = logging.getLogger(__name__)


class CSVFilter:
    """CSV行过滤器 - 流式处理超大文件"""
    
    def __init__(
        self,
        input_file: str,
        output_file: str,
        delimiter: str = ",",
        encoding: Optional[str] = None,
        chunk_size: int = 100000
    ):
        """
        初始化CSV过滤器
        
        Args:
            input_file: 输入CSV文件路径
            output_file: 输出CSV文件路径
            delimiter: CSV分隔符（默认 ,）
            encoding: 文件编码（默认自动检测）
            chunk_size: 每次处理的行数（默认10万行）
        """
        self.input_file = Path(input_file)
        self.output_file = Path(output_file)
        self.delimiter = delimiter
        self.encoding = encoding
        self.chunk_size = chunk_size
        
        # 统计信息
        self.total_rows = 0  # 输入总行数
        self.kept_rows = 0   # 保留的行数
        self.deleted_rows = 0  # 删除的行数
        self.file_size = 0
        
        self._validate_input_file()
    
    def _validate_input_file(self) -> None:
        """验证输入文件"""
        if not self.input_file.exists():
            raise FileNotFoundError(f"输入文件不存在: {self.input_file}")
        
        if not self.input_file.is_file():
            raise ValueError(f"输入路径不是文件: {self.input_file}")
        
        self.file_size = self.input_file.stat().st_size
        logger.info(f"输入文件: {self.input_file}")
        logger.info(f"文件大小: {self._format_size(self.file_size)}")
    
    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """格式化文件大小"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f}TB"
    
    def _detect_encoding(self) -> str:
        """自动检测文件编码"""
        if self.encoding:
            return self.encoding
        
        try:
            with open(self.input_file, 'rb') as f:
                raw_data = f.read(100000)
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
            if '.' in value:
                return float(value)
            else:
                return float(int(value))
        except (ValueError, TypeError):
            return None
    
    def filter_by_first_column_value(
        self,
        condition: Callable[[float], bool],
        keep_header: bool = True
    ) -> Tuple[int, int, int]:
        """
        根据第一列的条件过滤行
        
        Args:
            condition: 判断函数，返回True表示保留，False表示删除
            keep_header: 是否保留表头
            
        Returns:
            (总行数, 保留行数, 删除行数)
        """
        encoding = self._detect_encoding()
        
        logger.info(f"开始过滤...")
        start_time = datetime.now()
        
        try:
            with open(self.input_file, 'r', encoding=encoding, errors='replace') as infile, \
                 open(self.output_file, 'w', newline='', encoding='utf-8') as outfile:
                
                reader = csv.reader(infile, delimiter=self.delimiter)
                writer = csv.writer(outfile, delimiter=self.delimiter)
                
                # 处理表头
                try:
                    header = next(reader)
                    self.total_rows = 1
                    
                    if keep_header:
                        writer.writerow(header)
                        self.kept_rows += 1
                        logger.info(f"表头: {header}")
                    else:
                        logger.info(f"表头行将被删除: {header}")
                
                except StopIteration:
                    raise ValueError("CSV文件为空或无法读取")
                
                # 处理数据行
                for row_idx, row in enumerate(reader, start=2):
                    if not row:
                        continue
                    
                    self.total_rows += 1
                    
                    if len(row) == 0:
                        logger.warning(f"第{row_idx}行为空行，跳过")
                        continue
                    
                    first_col_value = self._try_to_number(row[0])
                    
                    if first_col_value is not None:
                        if condition(first_col_value):
                            writer.writerow(row)
                            self.kept_rows += 1
                        else:
                            self.deleted_rows += 1
                    else:
                        # 第一列不是数值，无法比较，默认保留
                        writer.writerow(row)
                        self.kept_rows += 1
                    
                    if self.total_rows % self.chunk_size == 0:
                        logger.info(
                            f"已处理 {self.total_rows:,} 行 | "
                            f"保留 {self.kept_rows:,} 行 | "
                            f"删除 {self.deleted_rows:,} 行"
                        )
            
            # 显示完成信息
            elapsed_time = datetime.now() - start_time
            output_size = self._format_size(self.output_file.stat().st_size)
            
            logger.info(f"\n{'='*60}")
            logger.info(f"✓ 过滤完成!")
            logger.info(f"  总行数: {self.total_rows:,}")
            logger.info(f"  保留行数: {self.kept_rows:,}")
            logger.info(f"  删除行数: {self.deleted_rows:,}")
            logger.info(f"  删除比例: {self.deleted_rows/max(self.total_rows-1, 1)*100:.2f}%")
            logger.info(f"  输出文件: {self.output_file}")
            logger.info(f"  输出大小: {output_size}")
            logger.info(f"  耗时: {elapsed_time}")
            logger.info(f"{'='*60}\n")
            
            return self.total_rows, self.kept_rows, self.deleted_rows
            
        except Exception as e:
            logger.error(f"过滤过程中出错: {e}")
            raise
    
    def filter_by_first_column_less_than(self, threshold: float, keep_header: bool = True) -> Tuple[int, int, int]:
        """删除第一列小于指定阈值的行"""
        logger.info(f"过滤条件: 删除第一列 < {threshold} 的行")
        return self.filter_by_first_column_value(
            lambda x: x >= threshold,
            keep_header=keep_header
        )
    
    def filter_by_first_column_greater_than(self, threshold: float, keep_header: bool = True) -> Tuple[int, int, int]:
        """删除第一列大于指定阈值的行"""
        logger.info(f"过滤条件: 删除第一列 > {threshold} 的行")
        return self.filter_by_first_column_value(
            lambda x: x <= threshold,
            keep_header=keep_header
        )
    
    def filter_by_first_column_equals(self, target_value: float, keep_header: bool = True) -> Tuple[int, int, int]:
        """删除第一列等于指定值的行"""
        logger.info(f"过滤条件: 删除第一列 == {target_value} 的行")
        return self.filter_by_first_column_value(
            lambda x: x != target_value,
            keep_header=keep_header
        )
    
    def filter_by_first_column_range(self, min_value: float, max_value: float, keep_header: bool = True) -> Tuple[int, int, int]:
        """保留第一列在指定范围内的行"""
        logger.info(f"过滤条件: 保留第一列 [{min_value}, {max_value}] 范围内的行")
        return self.filter_by_first_column_value(
            lambda x: min_value <= x <= max_value,
            keep_header=keep_header
        )
    
    def get_summary(self) -> dict:
        """获取过滤摘要"""
        return {
            'input_file': str(self.input_file),
            'output_file': str(self.output_file),
            'total_rows': self.total_rows,
            'kept_rows': self.kept_rows,
            'deleted_rows': self.deleted_rows,
            'output_size': self._format_size(self.output_file.stat().st_size) if self.output_file.exists() else 'N/A',
            'deleted_ratio': f"{self.deleted_rows/max(self.total_rows-1, 1)*100:.2f}%" if self.total_rows > 1 else '0%'
        }


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(
        description='高性能CSV行过滤工具 - 根据第一列的值删除行',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 删除第一列小于0的行
  python csv_filter.py input.csv output.csv --less-than 0
  
  # 删除第一列大于100的行
  python csv_filter.py input.csv output.csv --greater-than 100
  
  # 保留第一列在0-100范围内的行
  python csv_filter.py input.csv output.csv --range 0 100
  
  # 删除第一列等于某个值的行
  python csv_filter.py input.csv output.csv --equals 0
        """
    )
    
    parser.add_argument('input_file', help='输入CSV文件路径')
    parser.add_argument('output_file', help='输出CSV文件路径')
    parser.add_argument('--delimiter', default=',', help='CSV分隔符 (默认: ,)')
    parser.add_argument('--encoding', help='文件编码 (默认: 自动检测)')
    parser.add_argument('--chunk-size', type=int, default=100000, help='显示进度的行数间隔')
    
    filter_group = parser.add_mutually_exclusive_group(required=True)
    filter_group.add_argument('--less-than', type=float, help='删除第一列小于指定值的行')
    filter_group.add_argument('--greater-than', type=float, help='删除第一列大于指定值的行')
    filter_group.add_argument('--equals', type=float, help='删除第一列等于指定值的行')
    filter_group.add_argument('--range', type=float, nargs=2, help='保留第一列在指定范围内的行')
    
    args = parser.parse_args()
    
    try:
        filter_obj = CSVFilter(
            input_file=args.input_file,
            output_file=args.output_file,
            delimiter=args.delimiter,
            encoding=args.encoding,
            chunk_size=args.chunk_size
        )
        
        if args.less_than is not None:
            filter_obj.filter_by_first_column_less_than(args.less_than)
        elif args.greater_than is not None:
            filter_obj.filter_by_first_column_greater_than(args.greater_than)
        elif args.equals is not None:
            filter_obj.filter_by_first_column_equals(args.equals)
        elif args.range is not None:
            filter_obj.filter_by_first_column_range(args.range[0], args.range[1])
        
        summary = filter_obj.get_summary()
        
        print("\n过滤摘要:")
        print(f"  输入文件: {summary['input_file']}")
        print(f"  输出文件: {summary['output_file']}")
        print(f"  总行数: {summary['total_rows']:,}")
        print(f"  保留行数: {summary['kept_rows']:,}")
        print(f"  删除行数: {summary['deleted_rows']:,}")
        print(f"  删除比例: {summary['deleted_ratio']}")
        print(f"  输出大小: {summary['output_size']}")
        
        return 0
        
    except Exception as e:
        logger.error(f"程序错误: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())