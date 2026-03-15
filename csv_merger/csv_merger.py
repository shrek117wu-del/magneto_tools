#!/usr/bin/env python3
"""
高性能CSV文件合并工具
支持处理超大型CSV文件（10G+）
"""

import os
import sys
import csv
import argparse
import logging
from pathlib import Path
from typing import Optional, List, Tuple
from datetime import datetime
import chardet

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('csv_merger.log')
    ]
)
logger = logging.getLogger(__name__)


class CSVMerger:
    """
    CSV文件合并器 - 流式处理超大文件
    """
    
    def __init__(
        self,
        input_dir: str,
        output_file: str,
        pattern: str = "*.csv",
        delimiter: str = ",",
        chunk_size: int = 100000,
        encoding: Optional[str] = None
    ):
        """
        初始化CSV合并器
        
        Args:
            input_dir: 输入目录路径
            output_file: 输出CSV文件路径
            pattern: 文件匹配模式（默认 *.csv）
            delimiter: CSV分隔符（默认 ,）
            chunk_size: 每次处理的行数（默认10万行）
            encoding: 文件编码（默认自动检测）
        """
        self.input_dir = Path(input_dir)
        self.output_file = Path(output_file)
        self.pattern = pattern
        self.delimiter = delimiter
        self.chunk_size = chunk_size
        self.encoding = encoding
        
        # 统计信息
        self.total_rows = 0
        self.total_files = 0
        self.header = None
        
        self._validate_input_dir()
    
    def _validate_input_dir(self) -> None:
        """验证输入目录"""
        if not self.input_dir.exists():
            raise ValueError(f"输入目录不存在: {self.input_dir}")
        
        if not self.input_dir.is_dir():
            raise ValueError(f"输入路径不是目录: {self.input_dir}")
        
        logger.info(f"输入目录: {self.input_dir}")
        logger.info(f"输出文件: {self.output_file}")
    
    def _detect_encoding(self, file_path: Path) -> str:
        """自动检测文件编码"""
        if self.encoding:
            return self.encoding
        
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(100000)  # 读取前100KB用于检测
                result = chardet.detect(raw_data)
                encoding = result.get('encoding', 'utf-8')
                
                if encoding and encoding.lower() != 'ascii':
                    logger.debug(f"{file_path.name}: 检测到编码 {encoding}")
                    return encoding
        except Exception as e:
            logger.warning(f"编码检测失败 {file_path.name}: {e}")
        
        return 'utf-8'
    
    def _get_csv_files(self) -> List[Path]:
        """获取所有匹配的CSV文件，按名称排序"""
        files = sorted(self.input_dir.glob(self.pattern))
        
        if not files:
            logger.warning(f"未找到匹配 '{self.pattern}' 的文件")
            return []
        
        logger.info(f"找到 {len(files)} 个CSV文件")
        for f in files:
            logger.info(f"  - {f.name} ({self._get_file_size(f)})")
        
        return files
    
    @staticmethod
    def _get_file_size(file_path: Path) -> str:
        """获取文件大小（人类可读格式）"""
        size = file_path.stat().st_size
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.2f}{unit}"
            size /= 1024
        return f"{size:.2f}TB"
    
    def _read_file_lines(self, file_path: Path, skip_header: bool = False):
        """
        流式读取文件
        
        Args:
            file_path: 文件路径
            skip_header: 是否跳过第一行
            
        Yields:
            CSV行列表
        """
        encoding = self._detect_encoding(file_path)
        
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                reader = csv.reader(f, delimiter=self.delimiter)
                
                # 跳过头部
                if skip_header:
                    next(reader, None)
                
                for row in reader:
                    if row:  # 忽略空行
                        yield row
                        
        except Exception as e:
            logger.error(f"读取文件错误 {file_path.name}: {e}")
            raise
    
    def _validate_columns(self, file_path: Path, row: List[str]) -> bool:
        """验证列数一致性"""
        if self.header is None:
            self.header = row
            logger.info(f"设置表头: {len(self.header)} 列")
            return True
        
        if len(row) != len(self.header):
            logger.error(
                f"{file_path.name}: 列数不匹配! "
                f"期望 {len(self.header)} 列，实际 {len(row)} 列"
            )
            return False
        
        return True
    
    def merge(self) -> Tuple[int, int]:
        """
        执行合并操作
        
        Returns:
            (合并的文件数, 合并的总行数)
            
        Raises:
            ValueError: 如果文件列数不一致
        """
        csv_files = self._get_csv_files()
        
        if not csv_files:
            logger.error("没有找到CSV文件")
            return 0, 0
        
        start_time = datetime.now()
        logger.info(f"开始合并 ({start_time.strftime('%Y-%m-%d %H:%M:%S')})")
        
        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as outf:
                writer = csv.writer(outf, delimiter=self.delimiter)
                
                for file_idx, file_path in enumerate(csv_files):
                    logger.info(f"处理文件 [{file_idx + 1}/{len(csv_files)}]: {file_path.name}")
                    
                    is_first_file = (file_idx == 0)
                    file_row_count = 0
                    
                    for row_idx, row in enumerate(self._read_file_lines(
                        file_path, 
                        skip_header=not is_first_file
                    )):
                        # 第一个文件的第一行作为表头
                        if is_first_file and row_idx == 0:
                            if not self._validate_columns(file_path, row):
                                raise ValueError(f"文件 {file_path.name} 列数不匹配")
                            writer.writerow(row)
                        else:
                            if not self._validate_columns(file_path, row):
                                raise ValueError(f"文件 {file_path.name} 列数不匹配")
                            writer.writerow(row)
                        
                        file_row_count += 1
                        self.total_rows += 1
                        
                        # 每处理chunk_size行显示一次进度
                        if file_row_count % self.chunk_size == 0:
                            logger.info(
                                f"  已处理 {file_row_count:,} 行 | "
                                f"总计 {self.total_rows:,} 行"
                            )
                    
                    self.total_files += 1
                    logger.info(
                        f"  完成: {file_row_count:,} 行 | "
                        f"累计: {self.total_rows:,} 行"
                    )
            
            # 显示完成信息
            elapsed_time = datetime.now() - start_time
            output_size = self._get_file_size(self.output_file)
            
            logger.info(f"\n{'='*50}")
            logger.info(f"✓ 合并完成!")
            logger.info(f"  文件数: {self.total_files}")
            logger.info(f"  总行数: {self.total_rows:,}")
            logger.info(f"  输出文件: {self.output_file}")
            logger.info(f"  文件大小: {output_size}")
            logger.info(f"  耗时: {elapsed_time}")
            logger.info(f"{'='*50}\n")
            
            return self.total_files, self.total_rows
            
        except Exception as e:
            logger.error(f"合并过程中出错: {e}")
            raise
    
    def get_summary(self) -> dict:
        """获取合并摘要"""
        return {
            'total_files': self.total_files,
            'total_rows': self.total_rows,
            'output_file': str(self.output_file),
            'output_size': self._get_file_size(self.output_file) if self.output_file.exists() else 'N/A',
            'header_columns': len(self.header) if self.header else 0
        }


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(
        description='高性能CSV文件合并工具 - 支持处理超大型文件(10G+)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 合并目录下的所有CSV文件
  python csv_merger.py /data/csv_files -o output.csv
  
  # 指定文件模式和分隔符
  python csv_merger.py /data/csv_files -o output.csv --pattern "*.csv" --delimiter ","
  
  # 指定处理块大小和编码
  python csv_merger.py /data/csv_files -o output.csv --chunk-size 50000 --encoding utf-8
        """
    )
    
    parser.add_argument(
        'input_dir',
        help='输入目录路径（包含CSV文件）'
    )
    parser.add_argument(
        '-o', '--output',
        required=True,
        help='输出CSV文件路径'
    )
    parser.add_argument(
        '--pattern',
        default='*.csv',
        help='文件匹配模式 (默认: *.csv)'
    )
    parser.add_argument(
        '--delimiter',
        default=',',
        help='CSV分隔符 (默认: ,)'
    )
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=100000,
        help='每次处理的行数 (默认: 100000)'
    )
    parser.add_argument(
        '--encoding',
        help='文件编码 (默认: 自动检测)'
    )
    
    args = parser.parse_args()
    
    try:
        merger = CSVMerger(
            input_dir=args.input_dir,
            output_file=args.output,
            pattern=args.pattern,
            delimiter=args.delimiter,
            chunk_size=args.chunk_size,
            encoding=args.encoding
        )
        
        merger.merge()
        summary = merger.get_summary()
        
        print("\n合并摘要:")
        print(f"  文件数: {summary['total_files']}")
        print(f"  行数: {summary['total_rows']:,}")
        print(f"  输出大小: {summary['output_size']}")
        
        return 0
        
    except Exception as e:
        logger.error(f"程序错误: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())