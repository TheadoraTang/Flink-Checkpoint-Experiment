#### 初始化 Swarm 集群（所有主机）
将所有主机加入同一个 Swarm 集群：
1. **在主节点（运行 JobManager 的主机）初始化 Swarm**：
   ```bash
   # 在主节点执行（记录输出的 join 命令，用于从节点加入）
   sudo docker swarm init --advertise-addr 192.168.121.130
   ```

2. **在所有从节点执行主节点输出的 join 命令**：
   ```bash
   docker swarm join --token SWMTKN-1-4523jf5pxem9soyxza2fz7b2u7x964prah4micq4mlb71c8q57-98nvntpsj7cs2vspd8cgccrcn 192.168.121.130:2377
   ```
   （主节点若忘记 join 命令，可在主节点执行 `sudo docker swarm join-token worker` 重新获取）


