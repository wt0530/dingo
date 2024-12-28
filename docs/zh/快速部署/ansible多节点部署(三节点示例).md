# ansible多节点部署(三节点示例)

## 1 环境准备

操作系统： centos8.x版本

系统检查libc

```
ls -l /lib64/libc.so.6
输出: /lib64/libc.so.6 -> libc-2.28.so
```

确定主操作IP， ansible 部署操作均在主操作IP进行， 示例中主操作IP为192.168.0.200

### 1.1 非sudo用户设置sudo免密登录

如果每台机器密码一致，在inventory/hosts中使用了root用户名和密码，则不需要这一步操作

vim /etc/sudoers
```
dingo    ALL=(ALL)       NOPASSWD:ALL
```

### 1.2 ip免密登录

如果每台机器密码一致，在inventory/hosts中使用了root用户名和密码，则不需要这一步操作

```
# 生成ssh公钥和私钥
ssh-keygen -f ~/.ssh/id_rsa -N '' -t rsa -q -b 2048
 
# 将公钥copy到所有host节点， 包括本机节点
ssh-copy-id root@192.168.0.200
ssh-copy-id root@192.168.0.201
ssh-copy-id root@192.168.0.202
 
# 测试是否免密
ssh root@192.168.0.200
```

### 1.3 ansible安装

#### 1.3.1 在线安装

```
# 确保每台机器安装python环境 使用python3
 
# 主IP安装ansible， 如果已经安装过，则跳过
pip3 install pip -U
pip3 install ansible==2.9.27 # 固定版本号安装，非强制
 
 
pip3 install pyopenssl -U
```

#### 1.3.2 离线机器安装

```
# 前期文件准备：
wget -r -nH http://192.168.0.202:9000  # 一键下载所有需要的包文件
 
ansible_installer_centos8.sh 单独进行放置 进行ansible安装
其他文件放置到部署包dingo-deploy/artifacts目录下
 
#  安装ansible 需要root用户
bash ansible_installer_centos8.sh # 三台环境均需要执行， 确保每台机器的python3路径一致
 
#注意在部署前修改 inventory/hosts
ansible_python_interpreter=/usr/local/miniconda3/bin/python
 
 
# 以下所有的ansible和python命令  替换为全路径运行， 比如
/usr/local/miniconda3/bin/ansible-playbook playbook.yml
```

## 2 配置说明

### 2.1 Pull git部署代码

```
git clone https://github.com/dingodb/dingo-deploy.git
 
 
# 离线安装需手动下载zip包
```

### 2.2 部署配置

#### 2.2.1 配置安装选项

配置文件地址： dingo-deploy/group_vars/all/_shared.yml

```
# 修改安装目录
installer_root_path
 
# 修改用户组
dingo_user
dingo_group
jdk_user
jdk_group  # 现在配置jdk组和dingo组相同, 初始化mysql,java为必须安装选项
 
 
# 安装选项修改
install_system: true    # 安装系统环境，第一次安装必须打开，设置文件限制
install_java_sdk: true  # 安装jdk环境， jdk安装，第一次安装时打开，执行executor和mysql_init需要
install_dingo: true     # 安装executor
install_dingo_store: true   # 安装dingo-store,包括coordinator、store、mysql_init
--以下是安装监控所选配置
install_prometheus: true    # 安装prometheus数据库
install_node_exporter: true # 安装节点监控
install_process_exporter: true  # 安装进程监控
install_grafana: true  # 安装grafana监控界面

 
# 离线安装用户也要配置 install_system
install_system: true
install_system_basicTools: false
install_system_fileLimits: true   #文件描述符限制一定要打开， 只需要第一次安装时打开就可以开就可以
```

```
---
 
# define the root path of install
installer_root_path: /mnt/nvme0n1/jason/v8/dingo-store
installer_cache_path: /tmp
delete_cache_after_install: false
 
# define the global log and data directory
dingo_log_dir: "{{ installer_root_path }}/log"
dingo_data_dir: "{{ installer_root_path }}/data"
dingo_run_dir: "{{ installer_root_path }}/run"
 
dingo_user: jason
dingo_group: jason
jdk_user: jason
jdk_group: jason
 
#define installer password
installer_password: dingo
 
#-----------------------------------------------------
# 1. Update System Configuration about OS
#-----------------------------------------------------
install_system: true
# disann is must libaio-devel and boost-devel
install_system_basicTools: true
install_system_fileLimits: true
set_core_file: true
core_file_dir: /home/jason/corefiles
ulimit_nproc_limit: 4194304
ulimit_nofile_limit: 1048576
fs_aio_max_nr: 1048576
install_optimize_memory: true
install_no_passwd_login: true
#-----------------------------------------------------
# 2. Install Java SDK
#-----------------------------------------------------
install_java_sdk: true
jdk_install_path: /opt
jdk_home: "{{ jdk_install_path }}/jdk"
#-----------------------------------------------------
# 2_1. Check For Port Conflicts
#-----------------------------------------------------
check_port_conflicts: true
 
 
#-----------------------------------------------------
# 3. Install Dingo to Dingo home
#-----------------------------------------------------
install_dingo: true
install_dingo_basic_command: true
install_dingo_update_configuration: true
install_dingo_start_roles: true
# is license support
is_license_support: false
 
# support ldap(optional)
is_support_ldap: false
openldap_server_ip: localhost
openldap_server_port: 389
openldap_server_root_password: "123456"
openldap_server_bindDN: "cn=admin,dc=localdomain,dc=com"
openldap_server_baseDN: "dc=localdomain,dc=com"
 
 
#-----------------------------------------------------
# 4.Install Dingo-store to Dingo home
#-----------------------------------------------------
install_dingo_store: true
install_dingo_store_basic_command: true
install_dingo_store_update_configuration: true
install_dingo_store_start_roles: true
install_dingo_store_default_replica_num: 3
open_dingo_store_logrotate: false
 
 
#-----------------------------------------------------
# 5. Install Prometheus to Dingo directory
#-----------------------------------------------------
install_prometheus: true
blackbox_exporter_port: 19115
blackbox_exporter_server: "{{ prometheus_server }}"
 
# JMX_PROMETHEUS_JAVAAGENT_PORT
jmx_prometheus_javaagent_port: 8899
 
# Node Exporter
install_node_exporter: true
node_exporter_port: 19100
node_exporter_servers: "{{ groups['node_exporter'] }}"
 
# Process Exporter
install_process_exporter: true
process_exporter_port: 19256
process_exporter_servers: "{{ groups['process_exporter'] }}"
 
# Prometheus
prometheus_port: 19090
prometheus_server: "{{ groups['prometheus'][0] }}"
prometheus_url: "http://{{ prometheus_server }}:{{ prometheus_port }}/prometheus"
 
# Pushgateway
pushgateway_port: 19091
pushgateway_server: "{{ prometheus_server }}"
 
# Grafana
install_grafana: true
grafana_port: 3000
grafana_server: "{{ groups['grafana'][0] }}"
default_dashboard_uid: "RNezu0fWk"
 
dingo_tmp_coordinator_list: "{{ groups['coordinator'] }}"
dingo_tmp_store_list: "{{ groups['store'] }}"
dingo_tmp_index_list: "{{ groups['index'] }}"
dingo_tmp_executor_list: "{{ groups['executor'] | default(\"\") }}"
 
# dingo-store port
dingo_store_coordinator_exchange_port: 22001
dingo_store_coordinator_raft_port: 22101
dingo_store_exchange_port: 20001
dingo_store_raft_port: 20101
dingo_store_document_exchange_port: 23001
dingo_store_document_raft_port: 23101
dingo_store_index_exchange_port: 21001
dingo_store_index_raft_port: 21101
dingo_store_diskann_exchange_port: 24001
dingo_store_diskann_raft_port:  24101
# lisence ip
server_listen_host: 0.0.0.0
raft_listen_host: 0.0.0.0
 
 
# executer port
dingo_coordinator_exchange_port: "{{ dingo_store_coordinator_exchange_port }}"
dingo_executor_exchange_port: 8765
dingo_mysql_port: 3307
dingo_auto_increment_cache_count: 10000
dingo_executor_buffer_size: 67108864
dingo_executor_buffer_number: 2
dingo_executor_file_size: 67108864
 
# proxy port
dingo_proxy_http_port: 13000
dingo_proxy_grpc_port: 9999
 
# web port
install_monitor_web: false
dingo_monitor_backend_port: 13001
install_nginx: false
nginx_install_path: "{{ installer_root_path }}"
dingo_monitor_frontend_port: 13002
nginx_user: "{{ dingo_user }}"
nginx_group: "{{ dingo_group }}"
nginx_data_path:  "{{ nginx_install_path }}/nginx"
nginx_log_path: "{{ nginx_install_path }}/nginx"
nginx_run_path: "{{ nginx_install_path }}/nginx"
 
 
# define dingo coordinator http monitor port: 192.168.0.18:8080,192.168.0.19:8080,192.168.0.20:8080
dingo_coordinator_http_monitor_port: "{{ dingo_store_coordinator_exchange_port }}"
dingo_store_http_monitor_port: "{{ dingo_store_exchange_port }}"
dingo_index_http_monitor_port: "{{ dingo_store_index_exchange_port }}"
dingo_executor_http_monitor_port: "{{ jmx_prometheus_javaagent_port }}"
 
# ['192.168.01.10:9201','192.168.01.11:9201','192.168.01.12:9201']
dingo_coordinator_http_tmp_list: "{% for item in dingo_tmp_coordinator_list %} '{{item}}:{{ dingo_coordinator_http_monitor_port }}' {% endfor %}"
dingo_coordinator_http_monitor_list: "[ {{ dingo_coordinator_http_tmp_list.split() | join(\",\") | default(\"\") }} ]"
 
dingo_coordinator_exchange_tmp_list: "{% for item in dingo_tmp_coordinator_list %} '{{item}}:{{ dingo_store_coordinator_exchange_port }}' {% endfor %}"
dingo_coordinator_exchange_tmp_list_string: " {{ dingo_coordinator_exchange_tmp_list.split() | join(\",\") | default(\"\") }} "
 
dingo_coordinator_exchange_tmp_list_1: "{% for item in dingo_tmp_coordinator_list %} {{item}}:{{ dingo_store_coordinator_exchange_port }} {% endfor %}"
dingo_coordinator_exchange_tmp_list_string_1: " {{ dingo_coordinator_exchange_tmp_list_1.split() | join(\",\") | default(\"\") }} "
 
dingo_store_http_tmp_list: "{% for item in dingo_tmp_store_list %} '{{item}}:{{ dingo_store_http_monitor_port }}' {% endfor %}"
dingo_store_http_monitor_list: "[ {{ dingo_store_http_tmp_list.split() | join(\",\") | default(\"\") }} ]"
 
dingo_index_http_tmp_list: "{% for item in dingo_tmp_index_list %} '{{item}}:{{ dingo_index_http_monitor_port }}' {% endfor %}"
dingo_index_http_monitor_list: "[ {{ dingo_index_http_tmp_list.split() | join(\",\") | default(\"\") }} ]"
 
dingo_executor_http_tmp_list: "{% for item in dingo_tmp_executor_list %} '{{item}}:{{ dingo_executor_http_monitor_port }}' {% endfor %}"
dingo_executor_http_monitor_list: "[ {{ dingo_executor_http_tmp_list.split() | join(\",\") | default(\"\") }} ]"
```

#### 2.2.2 下载部署包

```
# 将以上配置的部署包下载至 dingo-deploy/artifacts目录下
wget -r -nH http://192.168.0.202:9000   一键下载所有文件， 
 
# 下载主要文件， 也可 wget http://192.168.0.202:9000/pack_name单独下载包文件
dingo-store: dingo-store.tar.gz 其中dingo-store.tar.gz 为poc版本, 需要切换到旧版本ansible部署
监控：  
  wget http://192.168.0.202:9000/blackbox_exporter-0.16.0.linux-amd64.tar.gz  
  wget http://192.168.0.202:9000/grafana-8.3.3.linux-amd64.tar.gz  
  wget http://192.168.0.202:9000/node_exporter-0.18.1.linux-amd64.tar.gz  
  wget http://192.168.0.202:9000/process-exporter-0.7.10.linux-amd64.tar.gz  
  wget http://192.168.0.202:9000/prometheus-2.14.0.linux-amd64.tar.gz  
  wget http://192.168.0.202:9000/pushgateway-1.0.0.linux-amd64.tar.gz
  wget http://192.168.0.202:9000/jmx_prometheus_javaagent-0.17.2.jar
 
jdk:  
  wget http://192.168.0.202:9000/jdk-8u171-linux-x64.tar.gz 
 
PS:
    ansible部署现在将dingo.zip和dingo-store.tar.gz合并为dingo.tar.gz, 并提供合并脚本： 下载以上两个包后在artifacts中执行 bash merge_dingo.sh
    如果仅部署测试dingo-store.tar.gz, 也可执行 bash merge_dingo.sh  单独转换为dingo.tar.gz, 不可以单独部署dingo.zip
 
executor:
   wget http://192.168.0.202:9000/dingo.zip  
 
 
# 如果想要获取最新版本的dingo-store.tar.gz
Dingo-2.0.0: wget https://work.dingodb.top/dingo-store.tar.gz # 获取最新tar包
```

#### 2.2.3 配置Hosts

配置文件地址： inventory/hosts

```
1. 配置[coordinator] 节点ip， 每个节点启动一个coordinator服务
2. 配置[store] 节点ip，每个节点默认启动一个， 如果启动多个， 设置store_num变量, 如果想要将store部署在不同的磁盘上 设置disk变量,以空格分开
3. 删除或注释 Dingo-0.5.0版本的配置 [executor]， [driver]， [web]
4. 配置监控工具的 节点ip
    [prometheus]：监控数据库， 配置单节点即可
    [grafana]： 监控界面， 配置单节点即可
    [node_exporter]： 节点监控， 系统性能， 为每个节点部署
    [process_exporter]：进程监控，进程性能， 为每个节点部署
```
```
[all:vars]
ansible_connection=ssh
#ansible_ssh_user=root
#ansible_ssh_pass=datacanvas@123
ansible_python_interpreter=/usr/bin/python3
 
#[add_coordinator]
# 192.168.0.203
 
#[add_store]
#192.168.0.203
 
 
[scaling_in_dingo:children]
add_coordinator
add_store
 
 
[coordinator]
192.168.0.101
192.168.0.104
192.168.0.106
 
[store]
# 192.168.0.201
# 192.168.0.201 store_num=2
# 192.168.0.201 store_num=2 disk='/home/sd1/store1 /home/sd2/store2'
192.168.0.101
192.168.0.104
192.168.0.106
 
[document]
# 192.168.0.201 document_num=2 disk='/home/sd1/document1 /home/sd2/document2'
192.168.0.101
192.168.0.104
192.168.0.106
 
[index]
# 192.168.0.201 index_num=2 disk='/home/sd1/index1 /home/sd2/index2'
192.168.0.101
192.168.0.104
192.168.0.106
 
[diskann]
192.168.0.101
 
[prometheus]
192.168.0.101
 
[grafana]
192.168.0.101
 
 
[all_nodes:children]
coordinator
store
index
 
[executor]
192.168.0.101
192.168.0.104
192.168.0.106
 
[proxy]
192.168.0.101
 
[web]
192.168.0.101
 
[executor_nodes:children]
executor
proxy
 
[node_exporter]
192.168.0.101
192.168.0.104
192.168.0.106
 
[process_exporter]
192.168.0.101
192.168.0.104
192.168.0.106
```

## 3 ansible部署与使用

### 3.1 部署

```
$ cd dingo-deploy
$ ansible-playbook playbook.yml

# 离线用户
$ cd dingo-deploy
$ /usr/local/miniconda3/bin/ansible-playbook playbook.yml
```

### 3.2 ansible使用.

#### 3.2.1 命令行使用.

```
$ cd dingo-deploy
$ ansible store -m ping # 测试store组所有ip, 可用于安装之前， 先确保每台机器ansible连接通
 
$ ansible store -m shell -a "ps -ef |grep dingodb_server" #  查看dingodb_server进程
 
$ ansible store -m shell -a "/path/scripts/start-coordinator.sh  start/stop/clean/restart/deploy/cleanstart" --become --become-user "myuser"  # 启动  coordinator节点  cleanstart包含 stop clean deploy start
$ ansible store -m shell -a "/path/scripts/start-store.sh  start/stop/clean/restart/deploy/cleanstart" --become --become-user "myuser"  # 启动  store节点
```

#### 3.2.2 管理工具DingoControl

```
# 前提: 已经配置完成group_vars/all/_shared.yml和hosts
# 对DingoControl 赋权限
chmod +x DingoControl
 
# DiongControl 命令详解
Options:
  --help        display this help and exit
  deploy        deploy the application 
  show          show all process for user
  stop          stop  server
  clean         clean server
  start         start server
  cleanstart    stop clean deploy  and start server
  install       install playbook
 
# install
  # 安装选项， playbook是安装默认配置执行， dingodb是安装dingo(executor)和dingo-store, monitor是安装所有监控工具， 其余都是单个步骤安装
  # 示例: ./DingoControl install playbook
  playbook    playbook.yml
  system      install system
  jdk         install jdk
  dingo       install dingo
  dingo-store install dingo-store
  dingodb     install dingo and dingo-store
  prometheus  install prometheus
  grafana     install grafana
  node        install node_exporter
  process     install process_exporter
  monitor     install prometheus grafana node_exporter process_exporter
 
 
 
# deploy
  # 部署模块包含coordinator的部署和store的部署，生成启动前的目录结构
  # 示例: ./DingoControl deploy all
  all               stop/clean/deploy store/coordinator
  store             stop/clean/deploy store
  coordinator       stop/clean/deploy coordinator  
 
# show
  # 示例: ./DingoControl show process
  process          show all process # 显示部署用户的所有进程
  file             show all file not user # 查看部署目录下是否有非部署用户权限的目录
 
# stop
  # 停止进程， 可以单个进程停止，也可全部停止
  # ./DingoControl stop store
  all               stop all process
  store             stop store
  coordinator       stop coordinator
  executor          stop executor
  node-exporter     stop node-exporter
  process-exporter  stop process-exporter
  prometheus        stop prometheus
  grafana           stop grafana
 
# clean
  # 清理文件，自动stop进程, 只有all为清理全部文件并删除目录， coordinator和store 是清空部署文件和数据， 监控模块是清理掉systemctl服务文件
  # 示例: ./DingoControl clean store
  all               stop all server and clean all file, if want del completely
  dingo-store       stop/clean store/coordinator and clean dingo-store, deprecated
  store             stop/clean store
  coordinator       stop/clean coordinator
  node-exporter     stop/clean node-exporter
  process-exporter  stop/clean process-exporter
  prometheus        stop/clean prometheus
  grafana           stop/clean grafana
 
 
# start
  # 启动进程， 当启动store时自动执行mysql_init.sh进行初始化表
  # 示例: ./DingoControl start store
  all               start all process
  store             start store
  coordinator       start coordinator
  executor          start executor
  node-exporter     start node-exporter
  process-exporter  start process-exporter
  prometheus        start prometheus
  grafana           start grafana
 
 
# cleanstart
  # 清空数据重新部署和启动coordinator、store和executor
  # 示例: ./DingoControl cleanstart all
  all   stop-clean-deploy store/coordinator and restart executor
```

##### 3.2.2.1coordinator扩缩容

目前ansible不支持单机器扩容，需要额外未部署的机器

```
# 扩容
1. 先进行新机器服务部署
# 配置inventory/hosts， 依照自己的需要打开以下两个选项
#[add_coordinator]
# 192.168.0.203
 
#[add_store]
#192.168.0.203
 
 
2. 执行部署新集群命令
./DingoControl install scaling_in_dingodb
 
 
3.执行raft addpeer, 填写最新扩容的ip地址， 如果扩容多个，需要多次执行， 默认的raft端口地址为22101
# 目前采取全部广播的方式，每次广播仅有leader节点会成功， 进行index=0和index=1两次广播
./DingoControl install addpeer 192.168.0.203
 
4. 修改coor_list, 注意以下格式以","分割
./DingoControl install coor_list "192.168.0.200:22001,192.168.0.201:22001,192.168.0.202:22001,192.168.0.203:22001"    
 
 
# 缩容
1. 缩容命令
# 会先将目标机器服务 停掉，然后进行其他节点广播，每次广播仅有leader节点会成功， 进行index=0和index=1两次广播
./DingoControl install scaling_out_dingodb 192.168.0.203
 

# 单机器启停store或者coordinator
./DingoControl start one_coordinator 192.168.0.203
./DingoControl start one_store 192.168.0.203
 
./DingoControl stop one_coordinator 192.168.0.203
./DingoControl stop one_store 192.168.0.203
```

## 4 查看集群状态

```
#  进入installer_root_path 配置的目录
# 查看coordinator状态 # 以下ip地址修改为自己宿主机的ip地址 cd dingo-store/build/bin
./dingodb_client_coordinator --method=GetCoordinatorMap 
 
# 输出结果
WARNING: Logging before InitGoogleLogging() is written to STDERR
E20230517 11:29:53.836163 1828522 coordinator_client.cc:320] [main] coordinator url is empty, try to use file://./coor_list
I20230517 11:29:53.850924 1828526 naming_service_thread.cpp:203] brpc::policy::FileNamingService("./coor_list"): added 3
I20230517 11:29:53.851150 1828522 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=0
I20230517 11:29:53.851306 1828522 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=1
I20230517 11:29:53.882277 1828529 coordinator_interaction.h:208] [SendRequestByService] name_service_channel_ connect with meta server finished. response errcode: 0, leader_addr: 192.168.0.201:22001
I20230517 11:29:53.882387 1828529 coordinator_client_function_coor.cc:496] [SendGetCoordinatorMap] SendRequest status=OK
I20230517 11:29:53.882423 1828529 coordinator_client_function_coor.cc:497] [SendGetCoordinatorMap] leader_location {
  host: "192.168.0.202"
  port: 22001
}
auto_increment_leader_location {
  host: "192.168.0.202"
  port: 22001
}
 
 
#  查看store状态 ./dingodb_client_coordinator --method=GetStoreMap
 
# 输出结果
WARNING: Logging before InitGoogleLogging() is written to STDERR
E20230517 11:30:12.944561 1828534 coordinator_client.cc:320] [main] coordinator url is empty, try to use file://./coor_list
I20230517 11:30:12.958177 1828539 naming_service_thread.cpp:203] brpc::policy::FileNamingService("./coor_list"): added 3
I20230517 11:30:12.958379 1828534 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=0
I20230517 11:30:12.958513 1828534 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=1
I20230517 11:30:12.968495 1828539 coordinator_interaction.h:208] [SendRequestByService] name_service_channel_ connect with meta server finished. response errcode: 0, leader_addr: 192.168.0.202:22001
I20230517 11:30:12.968590 1828539 coordinator_client_function_coor.cc:457] [SendGetStoreMap] SendRequest status=OK
I20230517 11:30:12.968619 1828539 coordinator_client_function_coor.cc:459] [SendGetStoreMap] epoch: 1003
storemap {
  epoch: 1003
  stores {
    id: 1201
    state: STORE_NORMAL
    server_location {
      host: "192.168.0.202"
      port: 20001
    }
    raft_location {
      host: "192.168.0.202"
      port: 20101
    }
    keyring: "TO_BE_CONTINUED"
    create_timestamp: 1683768684801
    last_seen_timestamp: 1684294210333
  }
  stores {
    id: 1101
    state: STORE_NORMAL
    server_location {
      host: "192.168.0.200"
      port: 20001
    }
    raft_location {
      host: "192.168.0.200"
      port: 20101
    }
    keyring: "TO_BE_CONTINUED"
    create_timestamp: 1683768687632
    last_seen_timestamp: 1684294210202
  }
  stores {
    id: 1001
    state: STORE_NORMAL
    server_location {
      host: "192.168.0.201"
      port: 20001
    }
    raft_location {
      host: "192.168.0.201"
      port: 20101
    }
    keyring: "TO_BE_CONTINUED"
    create_timestamp: 1683768692877
    last_seen_timestamp: 1684294207689
  }
}
I20230517 11:30:12.970433 1828539 coordinator_client_function_coor.cc:467] [SendGetStoreMap] store_id=1201 state=1 in_state=0 create_timestamp=1683768684801 last_seen_timestamp=1684294210333
I20230517 11:30:12.970482 1828539 coordinator_client_function_coor.cc:467] [SendGetStoreMap] store_id=1101 state=1 in_state=0 create_timestamp=1683768687632 last_seen_timestamp=1684294210202
I20230517 11:30:12.970510 1828539 coordinator_client_function_coor.cc:467] [SendGetStoreMap] store_id=1001 state=1 in_state=0 create_timestamp=1683768692877 last_seen_timestamp=1684294207689
I20230517 11:30:12.970541 1828539 coordinator_client_function_coor.cc:474] [SendGetStoreMap] DINGODB_HAVE_STORE_AVAILABLE, store_count=3
 
# 建表
../dingodb_client_coordinator --method=CreateTable  --name=test1
 
# 输出结果
WARNING: Logging before InitGoogleLogging() is written to STDERR
E20230517 11:34:04.153780 1828620 coordinator_client.cc:320] [main] coordinator url is empty, try to use file://./coor_list
I20230517 11:34:04.167536 1828630 naming_service_thread.cpp:203] brpc::policy::FileNamingService("./coor_list"): added 3
I20230517 11:34:04.167704 1828620 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=0
I20230517 11:34:04.167815 1828620 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=1
W20230517 11:34:04.174784 1828630 coordinator_interaction.h:199] [SendRequestByService] name_service_channel_ connect with meta server success by service name, connected to: 192.168.0.201:22001 found new leader: 192.168.0.202:22001
I20230517 11:34:04.202661 1828628 coordinator_client_function_meta.cc:314] [SendCreateTable] SendRequest status=OK
I20230517 11:34:04.202737 1828628 coordinator_client_function_meta.cc:315] [SendCreateTable] table_id {
  entity_type: ENTITY_TYPE_TABLE
  parent_entity_id: 2
  entity_id: 66006
}
 
# 查所有表
./dingodb_client_coordinator --method=GetTables
 
 
# 查单表
./dingodb_client_coordinator --method=GetTable  --id=66006
./dingodb_client_coordinator --method=GetTableRange  --id=66006
./dingodb_client_coordinator --method=GetTableByName  --name=test1
 
# 删除表
./dingodb_client_coordinator --method=DropTable  --name=test1
```

## 5 问题总结.

### 5.1 module 'lib' has no attribute 'X509_V_FLAG_CB_ISSUER_CHECK

```
pip3 install pip -U
pip3 install pyopenssl -U
```

