package com.ffcs.etl.flink.hudi.flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * 基于Flink SQL Connector实现：从Hudi表中加载数据，编写SQL查询
 */
public class ReadHudi {

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


		// 3-执行查询语句，读取流式读取Hudi表数据
		tableEnv.executeSql(
				"SELECT userId, name, createTime, address, age, sex, ts, partition_day FROM test_user_sink "
		).print() ;
	}

}
