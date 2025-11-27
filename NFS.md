### **一、主节点（192.168.121.130）：部署NFS服务端**
#### 1. 安装NFS服务
```bash
# Ubuntu/Debian
sudo apt update && sudo apt install -y nfs-kernel-server

```

#### 2. 创建共享目录
```bash
# 创建Flink共享目录（与代码中挂载路径对应）
sudo mkdir -p /shared/flink/checkpoints
sudo mkdir -p /shared/flink/datasets/nyc-taxi

# 复制现有数据到共享目录（确保从节点能访问数据集）
sudo cp -r ./docker/assets/datasets/nyc-taxi/* /shared/flink/datasets/nyc-taxi/

# 设置权限（Flink容器内用户UID/GID为9999）
sudo chown -R 9999:9999 /shared/flink
sudo chmod -R 755 /shared/flink
```

#### 3. 配置NFS共享
编辑NFS配置文件`/etc/exports`：
```bash
sudo vim /etc/exports
```
添加以下内容（允许从节点访问共享目录）：
```ini
# 允许从节点192.168.121.xxx读写访问，权限为9999:9999
/shared/flink 192.168.121.xxx(rw,sync,no_root_squash,all_squash,anonuid=9999,anongid=9999)
```
- `rw`：读写权限
- `sync`：同步写入磁盘
- `all_squash`：将所有访问用户映射为匿名用户
- `anonuid/anongid`：指定匿名用户的UID/GID为9999（与Flink容器用户一致）

#### 4. 生效配置并启动服务
```bash
# 生效共享配置
sudo exportfs -arv


sudo systemctl restart nfs-kernel-server
sudo systemctl enable nfs-kernel-server

```

#### 5. 开放防火墙端口
```bash
sudo ufw allow from 192.168.121.128 to any port nfs
sudo ufw reload

```

### **二、从节点：挂载NFS共享**
#### 1. 安装NFS客户端
```bash
# Ubuntu/Debian
sudo apt update && sudo apt install -y nfs-common
```

#### 2. 创建挂载目录
```bash
sudo mkdir -p /shared/flink/checkpoints
sudo mkdir -p /shared/flink/datasets/nyc-taxi
```

#### 3. 挂载NFS共享目录
```bash
# 挂载主节点的共享目录到从节点本地
sudo mount -t nfs 192.168.121.130:/shared/flink /shared/flink
```

#### 4. 验证挂载
```bash
# 查看挂载状态
df -h | grep flink

# 测试读写权限（创建测试文件）
sudo touch /shared/flink/checkpoints/test.txt
```

#### 2. 验证共享存储
启动主节点和从节点后，在主节点创建文件，检查从节点是否能同步访问：
```bash
# 主节点执行
sudo -u \#9999 touch /shared/flink/checkpoints/nfs-test.txt

# 从节点执行（应能看到文件）
ls -l /shared/flink/checkpoints/nfs-test.txt
```


开启防火墙端口：
```bash
sudo ufw allow 2377/tcp
sudo ufw allow 7946/tcp
sudo ufw allow 7946/udp
sudo ufw allow 4789/udp

```