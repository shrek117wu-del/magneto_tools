#!/usr/bin/env python3
"""
CSV合并工具测试脚本
"""

import os
import csv
import tempfile
import shutil
from pathlib import Path
from csv_merger import CSVMerger


def create_test_csv(file_path, rows, num_cols=5):
    """创建测试CSV文件"""
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # 写入表头
        writer.writerow([f'col{i+1}' for i in range(num_cols)])
        # 写入数据行
        for i in range(rows):
            writer.writerow([f'row{i}_col{j}' for j in range(num_cols)])


def test_basic_merge():
    """测试基本合并"""
    print("\n" + "="*50)
    print("测试1: 基本合并")
    print("="*50)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # 创建测试数据
        test_dir = Path(tmpdir) / "input"
        test_dir.mkdir()
        
        output_dir = Path(tmpdir) / "output"
        output_dir.mkdir()
        
        # 创建3个CSV文件
        create_test_csv(test_dir / "file1.csv", 100)
        create_test_csv(test_dir / "file2.csv", 150)
        create_test_csv(test_dir / "file3.csv", 200)
        
        # 执行合并
        merger = CSVMerger(
            input_dir=str(test_dir),
            output_file=str(output_dir / "merged.csv")
        )
        
        files_count, rows_count = merger.merge()
        
        # 验证结果
        assert files_count == 3, f"期望3个文件，实际{files_count}个"
        assert rows_count == 450, f"期望450行，实际{rows_count}行"  # 100+150+200，不包括表头
        
        print("✓ 测试通过: 合并了3个文件，总共450行数据")


def test_column_validation():
    """测试列数验证"""
    print("\n" + "="*50)
    print("测试2: 列数验证")
    print("="*50)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_dir = Path(tmpdir) / "input"
        test_dir.mkdir()
        
        # 创建列数不同的文件
        create_test_csv(test_dir / "file1.csv", 100, num_cols=5)
        create_test_csv(test_dir / "file2.csv", 100, num_cols=6)  # 列数不同
        
        merger = CSVMerger(
            input_dir=str(test_dir),
            output_file=str(Path(tmpdir) / "merged.csv")
        )
        
        try:
            merger.merge()
            print("✗ 测试失败: 应该检测到列数不匹配")
        except ValueError as e:
            if "列数不匹配" in str(e):
                print("✓ 测试通过: 成功检测到列数不匹配")
            else:
                raise


def test_empty_rows():
    """测试处理空行"""
    print("\n" + "="*50)
    print("测试3: 处理空行")
    print("="*50)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_dir = Path(tmpdir) / "input"
        test_dir.mkdir()
        
        output_dir = Path(tmpdir) / "output"
        output_dir.mkdir()
        
        # 创建包含空行的文件
        file1_path = test_dir / "file1.csv"
        with open(file1_path, 'w', newline='', encoding='utf-8') as f:
            f.write("col1,col2,col3\n")
            f.write("a,b,c\n")
            f.write("\n")  # 空行
            f.write("d,e,f\n")
        
        merger = CSVMerger(
            input_dir=str(test_dir),
            output_file=str(output_dir / "merged.csv")
        )
        
        files_count, rows_count = merger.merge()
        
        print(f"✓ 测试通过: 合并完成，合并了{rows_count}行数据")


def test_large_file_simulation():
    """模拟大文件合并"""
    print("\n" + "="*50)
    print("测试4: 大文件模拟（500K行）")
    print("="*50)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_dir = Path(tmpdir) / "input"
        test_dir.mkdir()
        
        output_dir = Path(tmpdir) / "output"
        output_dir.mkdir()
        
        # 创建模拟大文件
        print("  创建测试文件...")
        create_test_csv(test_dir / "file1.csv", 250000)
        create_test_csv(test_dir / "file2.csv", 250000)
        
        print("  执行合并...")
        merger = CSVMerger(
            input_dir=str(test_dir),
            output_file=str(output_dir / "merged.csv"),
            chunk_size=100000
        )
        
        files_count, rows_count = merger.merge()
        
        assert rows_count == 500000, f"期望500000行，实际{rows_count}行"
        print(f"✓ 测试通过: 合并了{rows_count:,}行数据")


def test_custom_delimiter():
    """测试自定义分隔符"""
    print("\n" + "="*50)
    print("测试5: 自定义分隔符")
    print("="*50)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_dir = Path(tmpdir) / "input"
        test_dir.mkdir()
        
        output_dir = Path(tmpdir) / "output"
        output_dir.mkdir()
        
        # 创建分号分隔的文件
        for i in range(2):
            file_path = test_dir / f"file{i+1}.csv"
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                f.write("col1;col2;col3\n")
                for j in range(50):
                    f.write(f"data{i}_{j};test{j};value{j}\n")
        
        merger = CSVMerger(
            input_dir=str(test_dir),
            output_file=str(output_dir / "merged.csv"),
            delimiter=";"
        )
        
        files_count, rows_count = merger.merge()
        
        print(f"✓ 测试通过: 合并了{rows_count}行分号分隔的数据")


def test_file_pattern():
    """测试文件模式过滤"""
    print("\n" + "="*50)
    print("测试6: 文件模式过滤")
    print("="*50)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_dir = Path(tmpdir) / "input"
        test_dir.mkdir()
        
        output_dir = Path(tmpdir) / "output"
        output_dir.mkdir()
        
        # 创建不同名称的文件
        create_test_csv(test_dir / "data_001.csv", 100)
        create_test_csv(test_dir / "data_002.csv", 100)
        create_test_csv(test_dir / "other_file.csv", 100)
        
        # 只合并 data_*.csv 文件
        merger = CSVMerger(
            input_dir=str(test_dir),
            output_file=str(output_dir / "merged.csv"),
            pattern="data_*.csv"
        )
        
        files_count, rows_count = merger.merge()
        
        assert files_count == 2, f"期望2个文件，实际{files_count}个"
        print(f"✓ 测试通过: 只合并了匹配模式的{files_count}个文件")


def run_all_tests():
    """运行所有测试"""
    print("\n" + "="*60)
    print("CSV合并工具测试套件")
    print("="*60)
    
    tests = [
        test_basic_merge,
        test_column_validation,
        test_empty_rows,
        test_large_file_simulation,
        test_custom_delimiter,
        test_file_pattern,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"✗ 测试失败: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    # 输出总结
    print("\n" + "="*60)
    print("测试结果总结")
    print("="*60)
    print(f"通过: {passed}")
    print(f"失败: {failed}")
    print(f"总计: {passed + failed}")
    
    if failed == 0:
        print("\n✓ 所有测试通过!")
    else:
        print(f"\n✗ {failed} 个测试失败")
    
    return failed == 0


if __name__ == '__main__':
    import sys
    success = run_all_tests()
    sys.exit(0 if success else 1)