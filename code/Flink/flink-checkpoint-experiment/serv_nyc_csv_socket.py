#!/usr/bin/env python3
import socket
import time
import sys
import os
from datetime import datetime

def parse_pickup_time(line, col_index=1):
    """从 CSV 行解析 pickup 时间（默认第2列）"""
    try:
        parts = line.strip().split(',')
        if col_index < len(parts):
            dt_str = parts[col_index].strip().strip('"')
            if '.' in dt_str:
                dt_str = dt_str.split('.')[0]
            return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    return None

def serve_csv_lines(csv_file_path, host="0.0.0.0", port=19091, timerate=60):
    """
    启动 TCP 服务端，等待 Flink 连接后，按时间倍率发送 CSV 数据。
    
    :param csv_file_path: CSV 文件路径
    :param host: 监听地址（默认 0.0.0.0，允许外部连接）
    :param port: 监听端口（默认 19091）
    :param timerate: 时间倍率（现实 1 秒 = 数据中 timerate 秒）
    """
    if not os.path.exists(csv_file_path):
        print(f"❌ 文件不存在: {csv_file_path}")
        sys.exit(1)

    # 读取并预处理 CSV（跳过表头）
    with open(csv_file_path, "r", encoding="utf-8") as f:
        lines = []
        header = f.readline().strip()
        if header and not header.replace(",", "").replace('"', '').replace('.', '').isdigit():
            print("⚠️  跳过表头行:", header)
            header_skipped = True
        else:
            f.seek(0)
            header_skipped = False

        for line in f:
            line = line.strip()
            if line:
                lines.append(line + "\n")

    # 解析每行的时间戳（使用第2列，索引=1）
    timed_lines = []
    for line in lines:
        raw_line = line.rstrip("\n")
        dt = parse_pickup_time(raw_line, col_index=1)
        if dt is not None:
            timed_lines.append((dt, line))
        else:
            # 无法解析时间的行放在最前（或可丢弃）
            timed_lines.append((datetime.min, line))

    # 按时间排序（确保顺序）
    timed_lines.sort(key=lambda x: x[0])
    lines = [line for _, line in timed_lines]

    print(f"📊 准备发送 {len(lines)} 行数据（已按 pickup 时间排序）")
    print(f"🔌 启动 TCP 服务端，监听 {host}:{port} ...")
    print("⏳ 等待 Flink 连接...")

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((host, port))
    server_sock.listen(1)

    try:
        conn, addr = server_sock.accept()
        print(f"✅ Flink 已连接: {addr}")

        # 开始按时间倍率发送
        if not lines:
            print("⚠️ 无有效数据行")
            return

        # 重新解析时间用于发送（只解析一次）
        events = []
        for line in lines:
            raw_line = line.rstrip("\n")
            dt = parse_pickup_time(raw_line, col_index=1)
            events.append((dt, line))

        # 过滤掉 datetime.min（无效时间）
        valid_events = [(dt, line) for dt, line in events if dt != datetime.min]
        if not valid_events:
            print("⚠️ 无有效时间戳的数据")
            return

        start_time = valid_events[0][0]
        current_sim_time = start_time
        index = 0
        total = len(valid_events)

        print(f"⏱️  开始回放（时间倍率: {timerate}x），起始时间: {start_time}")

        try:
            while index < total:
                window_end = current_sim_time + timedelta(seconds=timerate)
                batch = []
                while index < total and valid_events[index][0] <= window_end:
                    batch.append(valid_events[index][1])
                    index += 1

                # 发送整批
                for line in batch:
                    conn.sendall(line.encode("utf-8"))

                sent_count = len(batch)
                print(f"📤 发送 {sent_count} 行（模拟时间 {current_sim_time} → {window_end}）")

                # 现实世界等 1 秒
                time.sleep(1)
                current_sim_time = window_end

            print(f"✅ 全部 {total} 行发送完毕！")
        except BrokenPipeError:
            print("⚠️ Flink 提前断开连接")
    finally:
        conn.close()
        server_sock.close()


if __name__ == "__main__":
    import argparse
    from datetime import timedelta  # 新增导入

    parser = argparse.ArgumentParser(description="NYC Taxi CSV 服务端（供 Flink Socket 连接）")
    parser.add_argument("csv_file", help="NYC 出租车 CSV 文件路径，例如 nyc-taxi-2023-01.csv")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址 (默认 0.0.0.0)")
    parser.add_argument("--port", type=int, default=19091, help="监听端口 (默认 19091)")
    parser.add_argument("--timerate", type=int, default=60, help="时间倍率（现实 1 秒 = 数据中 N 秒，默认 60）")

    args = parser.parse_args()

    serve_csv_lines(
        csv_file_path=args.csv_file,
        host=args.host,
        port=args.port,
        timerate=args.timerate
    )