#!/usr/bin/env python3
"""
按 tpep_dropoff_datetime（第3列）对 NYC 出租车 CSV 文件排序。
适用于大文件（逐行读取，不全载入内存）。
输出文件名为原文件名 + ".sorted.csv"
"""

import csv
import sys
import os
from datetime import datetime

def parse_datetime(dt_str):
    """解析多种可能的 datetime 格式"""
    if not dt_str or dt_str.strip().lower() in ('', 'null'):
        return datetime.min  # 无效时间排最前（或可设为 max）
    
    dt_str = dt_str.strip()
    # 支持: "2023-01-01 00:00:00" 或 "2023-01-01 00:00:00.000"
    if '.' in dt_str:
        dt_str = dt_str.split('.')[0]
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        # 如果解析失败，返回最小时间（避免崩溃）
        print(f"⚠️ 无法解析时间: {dt_str}", file=sys.stderr)
        return datetime.min

def sort_csv_by_dropoff(input_path, output_path=None):
    if not os.path.exists(input_path):
        print(f"❌ 文件不存在: {input_path}")
        sys.exit(1)

    if output_path is None:
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}.sorted{ext}"

    print(f"📥 读取: {input_path}")
    
    header = None
    rows = []

    # 定义 pickup 时间的下限：2023-01-01 00:00:00
    pickup_start = datetime(2023, 1, 1)

    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)  # 读取表头
        
        # 找到 dropoff_datetime 列索引（通常为第3列，索引=2）
        dropoff_col_index = None
        pickup_col_index = None  # 新增：用于过滤
        for i, col in enumerate(header):
            if "dropoff" in col.lower() and "datetime" in col.lower():
                dropoff_col_index = i
            if "pickup" in col.lower() and "datetime" in col.lower():
                pickup_col_index = i
        
        if dropoff_col_index is None:
            print("⚠️ 未找到包含 'dropoff' 和 'datetime' 的列，尝试使用第3列 (索引=2)")
            dropoff_col_index = 2  # 默认第3列

        if pickup_col_index is None:
            print("⚠️ 未找到 pickup_datetime 列，尝试使用第2列 (索引=1)")
            pickup_col_index = 1  # 默认第2列

        print(f"🔍 使用列索引 {dropoff_col_index} 作为 tpep_dropoff_datetime")
        print(f"🔍 使用列索引 {pickup_col_index} 作为 tpep_pickup_datetime（用于过滤 >= 2023-01-01 00:00:00）")

        # 读取所有行并解析时间
        for row_num, row in enumerate(reader, start=2):
            if len(row) <= max(dropoff_col_index, pickup_col_index):
                print(f"⚠️ 第 {row_num} 行字段不足，跳过")
                continue

            # === 修改过滤逻辑：只保留 pickup_datetime >= 2023-01-01 00:00:00 的行 ===
            try:
                pickup_dt = parse_datetime(row[pickup_col_index])
                if pickup_dt < pickup_start:
                    continue
            except Exception:
                continue  # 解析失败则跳过
            # ========================================================

            dt = parse_datetime(row[dropoff_col_index])
            rows.append((dt, row))

    print(f"📊 共读取 {len(rows)} 行数据（pickup >= 2023-01-01），开始排序...")
    rows.sort(key=lambda x: x[0])  # 按解析后的时间排序

    print(f"📤 写入排序后文件: {output_path}")
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for _, row in rows:
            writer.writerow(row)

    print("✅ 排序完成！")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python sort_nyc_csv.py <input.csv> [output.csv]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    sort_csv_by_dropoff(input_file, output_file)