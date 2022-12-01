package com.ffcs.etl.flink.hudi.flinksql;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class Hudi2ES {
    public static void main(String[] args) {
        // 1-获取表执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings) ;

        // 2-创建输入表，TODO：加载Hudi表数据
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
                        "    'read.streaming.enabled' = 'true',\n" +
                        "    'read.streaming.check-interval' = '4'\n" +
                        "                        )"
        );


/*        // 3-执行查询语句，读取流式读取Hudi表数据
        tableEnv.executeSql(
                "SELECT userId, name, createTime, address, age, sex, ts, partition_day FROM test_user_sink "
        ) ;*/
        String createESSQL="CREATE TABLE test_user_es ( \n" +
                "     `userId` STRING,\n" +
                "     `name` STRING,\n" +
                "     `createTime` STRING,\n" +
                "      `address` STRING,\n" +
                "     `age` INT,\n" +
                "     `sex` INT,\n" +
                "     `ts` TIMESTAMP(3),\n" +
                "     `partition_day` STRING,\n" +
                "  PRIMARY KEY (userId) NOT ENFORCED \n" +
                ") WITH ( \n" +
                "  'connector' = 'elasticsearch-7', \n" +
                "  'hosts' = 'http://bigdata01:9200', \n" +
                "  'format' = 'json',\n"+
                "  'index' = 'test_user_es' \n" +
                ") ";
        tableEnv.executeSql(createESSQL);
        tableEnv.executeSql(
                " insert into test_user_es SELECT userId, name, createTime, address, age, sex, ts, partition_day FROM test_user_sink "
        ) ;


    }
}
