package com.ffcs.etl.flink.hudi.flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class Hudi2Doris {
    public static void main(String[] args) {
        // 1-获取表执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings) ;

        // 2-创建输入表，TODO：加载Hudi表数据
        tableEnv.executeSql(
                " CREATE TABLE test_user_source(\n" +
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
                        "    'read.streaming.enabled' = 'true',\n" +
                        "    'read.streaming.check-interval' = '4'\n" +
                        "                        )"
        );


        tableEnv.executeSql("CREATE TABLE test_user_doris (\n" +
                "                            `userId` STRING,\n" +
                "                            `name` STRING,\n" +
                "                            `createTime` STRING,\n" +
                "                             `address` STRING,\n" +
                "                            `age` INT,\n" +
                "                            `sex` INT,\n" +
                "                            `ts`  TIMESTAMP(3),\n" +
                "                            `partition_day` STRING,\n" +
                "                             `update_time` TIMESTAMP(3),\n"+
                "                             primary key (userId) not enforced \n" +
                "                        )\n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = 'bigdata02:18030',\n" +
                "      'table.identifier' = 'test_db.test_user_doris',\n" +
                "      'username' = 'root',\n" +
                "      'password' = 'ffcs@2022'\n" +
                ")\n");

        tableEnv.executeSql(
                " insert into test_user_doris SELECT userId, name, createTime, address, age, sex, ts, partition_day,CURRENT_TIMESTAMP as   update_time  FROM test_user_source "
        ) ;


    }


}
