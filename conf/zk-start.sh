#!/bin/bash
# ============================================================
# apache/kafka 官方镜像 ZooKeeper 模式启动脚本
# 官方 launch 脚本强制 KRaft 存储格式化,ZK 模式下直接生成
# server.properties 并启动 broker
# 依赖环境变量:
#   KAFKA_BROKER_ID            broker id (1/2/3)
#   KAFKA_HOST_PORT            映射到宿主机(Windows)的端口
#   KAFKA_ZOOKEEPER_CONNECT    ZK 连接串
#   KAFKA_HEAP_OPTS            JVM 堆设置
# ============================================================
set -e

HOSTNAME=$(hostname)

cat > /tmp/server.properties <<EOF
broker.id=${KAFKA_BROKER_ID}
zookeeper.connect=${KAFKA_ZOOKEEPER_CONNECT}
zookeeper.connection.timeout.ms=18000
listeners=INTERNAL://0.0.0.0:9092,HOST://0.0.0.0:${KAFKA_HOST_PORT}
advertised.listeners=INTERNAL://${HOSTNAME}:9092,HOST://localhost:${KAFKA_HOST_PORT}
listener.security.protocol.map=INTERNAL:PLAINTEXT,HOST:PLAINTEXT
inter.broker.listener.name=INTERNAL
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/tmp/kafka-logs
num.partitions=3
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:-2}
transaction.state.log.replication.factor=${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:-2}
transaction.state.log.min.isr=1
default.replication.factor=${KAFKA_DEFAULT_REPLICATION_FACTOR:-2}
min.insync.replicas=${KAFKA_MIN_INSYNC_REPLICAS:-1}
auto.create.topics.enable=false
log.retention.hours=24
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
group.initial.rebalance.delay.ms=0
EOF

exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties
