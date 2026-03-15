#!/usr/bin/env python3
"""
CSV分析工具测试脚本
"""

import os
import csv
import tempfile
from pathlib import Path
from csv_analyzer import CSVAnalyzer


def create_test_csv(file_path, rows, num_cols=5, include_floats=False):
    """创建测试CSV文件"""
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # 写入表头
        writer.writerow([f'col{i+1}' for i in range(num_cols)])
        
        # 写入数据行
        for i in range(rows):
            if include_floats and i % 2 == 0:
                # 写入浮点数
                row = [f'{i*10.5}' for _ in range(num_cols)]
            else:
                # 写入整数
                row = [str(i*10) for _ in range(num_cols)]
            writer.writerow(row)


def test_basic_analysis():
    """测试基本分析"""
    print("\n" + "="*60)
    print("测试1: 基本分析")
    print("="*60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = Path(tmpdir) / "test.csv"
        create_test_csv(test_file, 1000)
        
        analyzer = CSVAnalyzer(file_path=str(test_file))
        result = analyzer.analyze()
        
        # 验证结果
        assert result['total_rows'] == 1001, f"期望1001行，实际{result['total_rows']}行"
        assert result['data_rows'] == 1000, f"期望1000行数据，实际{result['data_rows']}行"
        assert result['total_columns'] == 5, f"期望5列，实际{result['total_columns']}列"
        
        print("✓ 测试通过: 正确计算了行数和列数")


def test_numeric_range():
    """测试数值范围计算"""
    print("\n" + "="*60)
    print("测试2: 数值范围计算")
    print("="*60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = Path(tmpdir) / "test.csv"
        create_test_csv(test_file, 100)
        
        analyzer = CSVAnalyzer(file_path=str(test_file))
        result = analyzer.analyze()
        
        # 检查第一列的范围
        col0_stats = result['columns_stats_detailed']['0']
        assert col0_stats['is_numeric'], "第一列应该是数值"
        assert col0_stats['min_value'] == 0, f"最小值应为0，实际为{col0_stats['min_value']}"
        assert col0_stats['max_value'] == 990, f"最大值应为990，实际为{col0_stats['max_value']}"
        
        print(f"✓ 测试通过: 正确计算了数值范围")
        print(f"  最���值: {col0_stats['min_value']}")
        print(f"  最大值: {col0_stats['max_value']}")


def test_float_detection():
    """测试浮点数检测"""
    print("\n" + "="*60)
    print("测试3: 浮点数检测")
    print("="*60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = Path(tmpdir) / "test.csv"
        create_test_csv(test_file, 100, include_floats=True)
        
        analyzer = CSVAnalyzer(file_path=str(test_file))
        result = analyzer.analyze()
        
        col0_stats = result['columns_stats_detailed']['0']
        assert col0_stats['data_type'] in ['float', 'mixed'], \
            f"应检测为float或mixed，实际为{col0_stats['data_type']}"
        
        print(f"✓ 测试通过: 正确检测到浮点数")
        print(f"  数据类型: {col0_stats['data_type']}")


def test_missing_values():
    """测试处理缺失值"""
    print("\n" + "="*60)
    print("测试4: 处理缺失值")
    print("="*60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = Path(tmpdir) / "test.csv"
        
        with open(test_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['col1', 'col2', 'col3'])
            
            # 写入包含缺失值的数据
            for i in range(100):
                if i % 10 == 0:
                    row = ['', str(i*10), str(i*20)]  # col1为空
                else:
                    row = [str(i*5), str(i*10), str(i*20)]
                writer.writerow(row)
        
        analyzer = CSVAnalyzer(file_path=str(test_file))
        result = analyzer.analyze()
        
        col0_stats = result['columns_stats_detailed']['0']
        # 应该有10个缺失值
        assert col0_stats['valid_rows'] == 90, \
            f"期望90个有效值，实际{col0_stats['valid_rows']}个"
        
        print(f"✓ 测试通过: 正确处理了缺失值")
        print(f"  有效数据: {col0_stats['valid_rows']}/{col0_stats['total_rows']}")


def test_custom_delimiter():
    """测试自定义分隔符"""
    print("\n" + "="*60)
    print("测试5: 自定义分隔符")
    print("="*60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = Path(tmpdir) / "test.csv"
        
        with open(test_file, 'w', newline='', encoding='utf-8') as f:
            f.write("col1;col2;col3\n")
            for i in range(100):
                f.write(f"{i*5};{i*10};{i*20}\n")
        
        analyzer = CSVAnalyzer(
            file_path=str(test_file),
            delimiter=";"
        )
        result = analyzer.analyze()
        
        assert result['total_columns'] == 3, \
            f"期望3列，实际{result['total_columns']}列"
        
        print("✓ 测试通过: 正确处理了分号分隔符")


def test_large_file_simulation():
    """模拟大文件分析"""
    print("\n" + "="*60)
    print("测试6: 大文件模拟 (100K行)")
    print("="*60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = Path(tmpdir) / "test.csv"
        
        print("  创建测试文件...")
        create_test_csv(test_file, 100000)
        
        print("  执行分析...")
        analyzer = CSVAnalyzer(
            file_path=str(test_file),
            chunk_size=20000
        )
        result = analyzer.analyze()
        
        assert result['total_rows'] == 100001, \
            f"期望100001行，实际{result['total_rows']}行"
        
        print(f"✓ 测试通过: 分析了{result['data_rows']:,}行数据")
        print(f"  处理速度: {result['rows_per_second']:,.0f} 行/秒")


def test_analyze_multiple_columns():
    """测试分析多列"""
    print("\n" + "="*60)
    print("测试7: 分析多列")
    print("="*60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = Path(tmpdir) / "test.csv"
        create_test_csv(test_file, 100, num_cols=10)
        
        analyzer = CSVAnalyzer(
            file_path=str(test_file),
            analyze_columns=5
        )
        result = analyzer.analyze()
        
        assert result['analyzed_columns'] == 5, \
            f"期望分析5列，实际{result['analyzed_columns']}列"
        assert len(result['columns_stats_detailed']) == 5, \
            f"期望5列统计，实际{len(result['columns_stats_detailed'])}列"
        
        print("✓ 测试通过: 正确分析了多列")


def run_all_tests():
    """运行所有测试"""
    print("\n" + "="*60)
    print("CSV分析工具测试套件")
    print("="*60)
    
    tests = [
        test_basic_analysis,
        test_numeric_range,
        test_float_detection,
        test_missing_values,
        test_custom_delimiter,
        test_large_file_simulation,
        test_analyze_multiple_columns,
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
    print(f"���计: {passed + failed}")
    
    if failed == 0:
        print("\n✓ 所有测试通过!")
    else:
        print(f"\n✗ {failed} 个测试失败")
    
    return failed == 0


if __name__ == '__main__':
    import sys
    success = run_all_tests()
    sys.exit(0 if success else 1)