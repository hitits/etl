package com.ffcs.etl.test;

/**
 * @className: Test
 * @author: linzy
 * @date: 2022/11/22
 **/
public class Test {
    public static void main(String[] args) {
       String data =  " CREATE TABLE test_hudi_user_json(\n" +
                "                            `userId` STRING,\n" +
                "                            `name` STRING,\n" +
                "                            `createTime` STRING,\n" +
                "                            `address` STRING,\n" +
                "                            `age` INT,\n" +
                "                            `sex`  INT,\n" +
                "                            `ts` TIMESTAMP(3),\n" +
                "                            `image` VARBINARY,\n" +
                "                            `partitionDay` STRING,\n" +
                "                             primary key (userId) not enforced \n" +
                "                        )\n";
        System.out.println(data);
    }
}
