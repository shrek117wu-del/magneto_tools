#!/usr/bin/env python3
"""
CSV分析工具使用示例
"""

from csv_analyzer import CSVAnalyzer
import json


def example_1_basic_analysis():
    """示例1: 基本分析"""
    print("="*60)
    print("示例1: 基本分析小型CSV文件")
    print("="*60)
    
    analyzer = CSVAnalyzer(
        file_path='data.csv'
    )
    
    result = analyzer.analyze()
    print(f"\n分析结果摘要:")
    print(f"  总行数: {result['total_rows']:,}")
    print(f"  数据行数: {result['data_rows']:,}")
    print(f"  处理速度: {result['rows_per_second']:,.0f} 行/秒")


def example_2_large_file():
    """示例2: 分析大文件"""
    print("\n" + "="*60)
    print("示例2: 分析超大文件(10G+)")
    print("="*60)
    
    analyzer = CSVAnalyzer(
        file_path='/large_data/huge_file.csv',
        chunk_size=500000  # 每50万行显示一次进度
    )
    
    result = analyzer.analyze()
    
    # 逐列显示详细信息
    print("\n详细统计信息:")
    for col_name, col_stats in result['columns_stats_detailed'].items():
        print(f"\n列 {col_stats['column_index']}: {col_stats['column_name']}")
        print(f"  范围: [{col_stats['min_value']}, {col_stats['max_value']}]")
        print(f"  有效数据: {col_stats['valid_ratio']}")


def example_3_custom_delimiter():
    """示例3: 自定义分隔符"""
    print("\n" + "="*60)
    print("示例3: 处理分号分隔的文件")
    print("="*60)
    
    analyzer = CSVAnalyzer(
        file_path='data_semicolon.csv',
        delimiter=';'
    )
    
    result = analyzer.analyze()
    return result


def example_4_analyze_more_columns():
    """示例4: 分析更多列"""
    print("\n" + "="*60)
    print("示例4: 分析前5列而不是前3列")
    print("="*60)
    
    analyzer = CSVAnalyzer(
        file_path='data.csv',
        analyze_columns=5
    )
    
    result = analyzer.analyze()
    return result


def example_5_export_json():
    """示例5: 导出为JSON"""
    print("\n" + "="*60)
    print("示例5: 导出分析结果为JSON")
    print("="*60)
    
    analyzer = CSVAnalyzer(
        file_path='data.csv'
    )
    
    result = analyzer.analyze()
    
    # 保存为JSON文件
    with open('analysis_result.json', 'w', encoding='utf-8') as f:
        # 转换float为字符串以保持精度
        def convert_floats(obj):
            if isinstance(obj, float):
                return f"{obj:.6g}"
            elif isinstance(obj, dict):
                return {k: convert_floats(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_floats(v) for v in obj]
            return obj
        
        json_result = convert_floats(result)
        json.dump(json_result, f, indent=2, ensure_ascii=False)
    
    print("\n✓ 结果已保存到 analysis_result.json")


def example_6_batch_analysis():
    """示例6: 批量分析多个文件"""
    print("\n" + "="*60)
    print("示例6: 批量分析多个CSV文件")
    print("="*60)
    
    from pathlib import Path
    
    csv_dir = Path('./data')
    csv_files = list(csv_dir.glob('*.csv'))
    
    results = {}
    
    for csv_file in csv_files:
        print(f"\n分析: {csv_file.name}")
        
        try:
            analyzer = CSVAnalyzer(file_path=str(csv_file))
            result = analyzer.analyze()
            results[csv_file.name] = result
        except Exception as e:
            print(f"  ✗ 失败: {e}")
    
    # 汇总统计
    print("\n" + "-"*60)
    print("汇总统计:")
    print("-"*60)
    
    total_rows = sum(r['data_rows'] for r in results.values())
    total_size = sum(r['file_size'] for r in results.values())
    
    print(f"文件总数: {len(results)}")
    print(f"总行数: {total_rows:,}")
    print(f"总大小: {CSVAnalyzer._format_size(total_size)}")
    
    return results


if __name__ == '__main__':
    # 根据需要运行相应的示例
    
    # example_1_basic_analysis()
    # example_2_large_file()
    # example_3_custom_delimiter()
    # example_4_analyze_more_columns()
    # example_5_export_json()
    # example_6_batch_analysis()
    
    print("请取消注释要运行的示例...")