package com.ffcs.etl.flink.hudi.flinksql;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;


public class Kafka2Hudi {
    public static void main(String[] args) {

        //1.获取表的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设置为1
        env.setParallelism(1);
        //TODO: 由于增量将数据写入到Hudi表，所以需要启动Flink Checkpoint 检查点
        env.enableCheckpointing(5 * 1000);


        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()//设置流式模式
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        //2.创建输入表，TODO:从kafka消费数据
        tableEnv.executeSql(
                "CREATE TABLE user_kafka_source(\n" +
                        "                            `userId` STRING,\n" +
                        "                            `name` STRING,\n" +
                        "                            `createTime` STRING,\n" +
                        "                             `address` STRING,\n" +
                        "                            `age` INT,\n" +
                        "                            `sex` INT\n" +
                        "                        )\n" +
                        "                        WITH(\n" +
                        "                            'connector' = 'kafka',\n" +
                        "                            'topic'='test-user',\n" +
                        "                            'properties.bootstrap.servers' = 'bigdata03:9092,bigdata02:9092,bigdata01:9092',\n" +
                        "                            'properties.group.id' = 'test',\n" +
                        "                            'scan.startup.mode' = 'earliest-offset',\n" +
                        "                            'format' = 'json',\n" +
                        "                            'json.fail-on-missing-field' = 'false',\n" +
                        "                            'json.ignore-parse-errors' = 'true'\n" +
                        "                        )"
        );

        //3.转换数据，可以使用SQL，也可以是TableAPI
        Table etlTable = tableEnv
                .from("user_kafka_source")

                //添加字段：hudi数据合并的字段，时间戳
                .addColumns(
                        $("createTime").as("ts")
                )
                //添加字段：Hudi表分区字段，"ime": 2022-01-12 22:21:13.124
                .addColumns(
                        $("createTime").substring(0, 10).as("partition_day")
                );


        tableEnv.createTemporaryView("tmp_user", etlTable);

        //4.创建输出表，TODO:关联到hudi表，指定hudi表名称，存储路径，字段名称等信息

        tableEnv.executeSql(
                " CREATE TABLE test_user_sink(\n" +
                        "                            `userId` STRING,\n" +
                        "                            `name` STRING,\n" +
                        "                            `createTime` STRING,\n" +
                        "                             `address` STRING,\n" +
                        "                            `age` INT,\n" +
                        "                            `sex` INT,\n" +
                        "                            `ts`  TIMESTAMP(3),\n" +
                        "                            `partition_day` STRING,\n" +
                        "                             primary key (userId) not enforced \n" +
                        "                        )\n" +
                        "                        PARTITIONED BY (partition_day)\n" +
                        "                        WITH(\n" +
                        "                            'connector' = 'hudi',\n" +
                        "                            'path'='hdfs://ffcluster:8020/hudi-warehouse/test_user_hudi2hive',\n" +
                        "                            'table.type' = 'COPY_ON_WRITE',\n" +
                        "                            'hive_sync.enable'='true',\n" +
                        "                            'hive_sync.table'='test_user_hive4hudi', \n" +
                        "                            'hive_sync.db'='hudi_dbs', \n" +
                        "                            'hive_sync.mode' = 'hms',\n" +
                        "                            'hive_sync.metastore.uris' = 'thrift://bigdata01:9083', " +
                        "                            'write.operation' = 'upsert',\n" +
                        "                            'hoodie.datasource.write.recordkey.field' = 'userId',\n" +
                        "                            'write.precombine.field' = 'ts',\n" +
                        "                            'write.tasks' = '1'\n" +
                        "                        )"
        );

        //5.通过子查询的方式，将数据写入输出表
        tableEnv.executeSql(
                "INSERT INTO test_user_sink " +
                        "SELECT userId, name, createTime, address, age, sex, TO_TIMESTAMP(ts,'yyyy-MM-dd HH:mm:ss') ts , partition_day FROM tmp_user"
        );

    }
}