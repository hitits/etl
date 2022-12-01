package com.ffcs.etl.flink.hudi.flinksql;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Hive2Hive {

    public static void main(String[] args) throws Exception {

       // ParameterTool params = ParameterTool.fromArgs(args);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner() // 使用BlinkPlanner
                .inBatchMode() // Batch模式，默认为StreamingMode
                .build();


        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name = "myhive";                                 // Catalog名称，定义一个唯一的名称表示
     //   String defaultDatabase = params.get("defaultDatabase"); // 默认数据库名称
     //   String hiveConfDir = params.get("hiveConf");            // hive-site.xml路径
        String defaultDatabase = "hudi_dbs"; // 默认数据库名称
        String hiveConfDir = "/opt/soft/hive-3.1.2/conf";            // hive-site.xml路径
        String version = "3.1.2";                               // Hive版本号
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hive);
        tableEnv.useCatalog("myhive");



        //      如果hive中已经存在了相应的表，则这段代码省略
/*        String hiveSql = "create table if not exists hudi_dbs.ods_test_user(\n" +
                " `userId` string,\n" +
                " `name` string,\n" +
                " `createTime` string,\n" +
                "  `address` string,\n" +
                " `age` INT,\n" +
                " `sex` INT,\n" +
                " `ts`  TIMESTAMP(3) \n" +
                ") PARTITIONED BY (partition_day  string) \n " +
                "stored as parquet \n" +
                "TBLPROPERTIES (\n" +
                "  'partition.time-extractor.timestamp-pattern'='$dt $h:$m:00',\n" +
                "  'sink.partition-commit.delay'='0s',\n" +
                "  'sink.partition-commit.trigger'='partition-time',\n" +
                "  'sink.partition-commit.policy.kind'='metastore'" +
                ")";
        tableEnv.executeSql(hiveSql);*/





        String insertSql = "insert into hudi_dbs.ods_test_user SELECT userid, name,age,address ,sex,ts,createtime ,partition_day FROM hudi_dbs.test_user_hive4hudi ";

        tableEnv.executeSql(insertSql);



 /*       TableResult result;
        String SelectTables_sql ="select * from hudi_dbs.test_user_hive4hudi";
        result = tableEnv.executeSql(SelectTables_sql);

        result.print();*/
    }
}
