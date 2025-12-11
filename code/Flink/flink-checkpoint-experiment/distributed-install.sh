#!/bin/bash

# 分布式安装脚本 distributed-install.sh
# 功能：
#   - 通用文件复制到所有 slave
#   - 专属目录 ./docker/slave-XX 作为子目录，仅复制到同名 slave，并保留目录结构

set -e

SLAVES=("slave-01" "slave-02" "slave-03" "slave-04")
REMOTE_BASE_DIR='~/flink-checkpoint-taskmanager/'

FILES_TO_COPY=(
    "docker/assets/datasets/nyc-taxi/output/empty.txt"
    "docker/assets/flink-lib/flink-s3-fs-hadoop-1.19.3.jar"
    "docker/assets/flink-conf.yaml"
    "slaves-restart.sh"
    "slaves-start.sh"
    "slaves-stop.sh"
    # 添加更多通用项...
)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "[INFO] 当前工作目录: $SCRIPT_DIR"

for slave in "${SLAVES[@]}"; do
    echo "----------------------------------------"
    echo "[INFO] 正在处理主机: $slave"

    # 1. 清理并重建远程基础目录
    ssh "$slave" "rm -rf ${REMOTE_BASE_DIR} && mkdir -p ${REMOTE_BASE_DIR}"

    # 2. 复制通用文件/目录（保留原始相对路径结构）
    for item in "${FILES_TO_COPY[@]}"; do
        local_path="$SCRIPT_DIR/$item"
        if [ ! -e "$local_path" ]; then
            echo "[WARN] 本地路径不存在，跳过: $local_path"
            continue
        fi

        # 计算远程目标目录（保留 item 的目录结构）
        remote_subdir="${REMOTE_BASE_DIR}$(dirname "$item")"
        echo "[INFO] 复制通用项: $item → $slave:$remote_subdir/"

        # 确保远程子目录存在（注意：不要用单引号，否则 ~ 无法展开）
        ssh "$slave" "mkdir -p $remote_subdir"
        scp -r "$local_path" "$slave:$remote_subdir/"
    done

    # 3. 复制专属目录（保留目录名，如 slave-01/）
    special_dir="$SCRIPT_DIR/docker/$slave"
    if [ -d "$special_dir" ]; then
        echo "[INFO] 发现专属目录: $special_dir，正在完整复制到 $slave..."
        # 确保远程 docker 目录存在
        ssh "$slave" "mkdir -p ${REMOTE_BASE_DIR}/docker"
        # 复制到 docker/ 子目录下，保留 slave-XX 名称
        scp -r "$special_dir" "$slave:${REMOTE_BASE_DIR}/docker/"
    else
        echo "[INFO] 无专属目录 $special_dir，跳过。"
    fi

    echo "[INFO] 主机 $slave 部署完成。"
done

echo "----------------------------------------"
echo "[SUCCESS] 所有 slave 节点部署完成！"