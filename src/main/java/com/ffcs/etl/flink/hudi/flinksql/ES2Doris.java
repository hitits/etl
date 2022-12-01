package com.ffcs.etl.flink.hudi.flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ES2Doris {
    public static void main(String[] args) {
        // 1-获取表执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings) ;

        tableEnv.executeSql("CREATE TABLE test_user_es2doris (\n" +
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
                "      'table.identifier' = 'test_db.test_user_es2doris',\n" +
                "      'username' = 'root',\n" +
                "      'password' = 'ffcs@2022'\n" +
                ")\n");


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
                " insert into test_user_es2doris SELECT userId, name, createTime, address, age, sex, ts, partition_day,CURRENT_TIMESTAMP as   update_time FROM test_user_es "
        ) ;


    }
}
