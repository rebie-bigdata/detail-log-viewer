# DetailLogViewer-用户流水数据查询方案

## 简介

### 解决的难题

1. 单个用户数据检索慢的问题
2. 大量用户数据管理复杂的问题
3. 单个用户具有多个标识的分页和读取麻烦的问题 
4. 数据初始化速度慢，恢复麻烦的问题
5. 以前的系统较难对接实时系统的问题

### 技术选择

1. 数据储存方案采用Phoenix+HBase的方案
2. 使用Spark来写入phoenix
3. 使用Phoenix的thin-server来读取数据

### 需要注意的问题

1. 写入HBase时仍旧使用的是HBase的API,而不是BulkLoad的方式
2. 修改表结构时对读方影响较大
3. 数据表需要合适的区分列族以保证性能和可靠性

## 实施方案

### 模型设计

1. PK的设计
    1. PK的组成
    2. PK的顺序
    3. 为什么要签名
2. 写入前的准备工作
    1. 修改表结构（如果有必要的话）
    2. 过滤部分脏数据（主要是PK局部为空）

### 写入操作

1. 使用Phoenix批量写入
2. 存在的问题：写入性能需要暴改

### 读取操作

1. 引入Phoenix-4.13-thin-client.jar
2. 使用url:```jdbc:phoenix:thin:url=http://phoenix_host:8765;serialization=PROTOBUF```