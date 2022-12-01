package com.ffcs.etl.flink.sink;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ffcs.etl.entity.enumerate.FiledTypeEnum;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * @className: MyDorisSink
 * @author: linzy
 * @date: 2022/11/29
 **/
public class MyDorisSink extends RichSinkFunction<JSONObject> {
    private String host;
    private Integer port;
    private String db;
    private String user;
    private String passwd;
    private Integer secondInterval;
    private volatile boolean isRunning = true;
    private Connection connection;
    private PreparedStatement preparedStatement;
    private Statement stmt;

    public MyDorisSink(String host, Integer port, String db, String user, String passwd, Integer secondInterval) {
        this.host = host;
        this.port = port;
        this.db = db;
        this.user = user;
        this.passwd = passwd;
        this.secondInterval = secondInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + db + "?useUnicode=true&characterEncoding=utf8", user, passwd);
        stmt = connection.createStatement();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (stmt != null) {
            stmt.close();
        }
    }

    @Override
    public void invoke(JSONObject json, Context context) {
        try {
            // String values = json.getString("values");
            JSONArray values = json.getJSONArray("values");
            String targetColsType = json.getString("targetColsType");
            String executeSQL = json.getString("executeSQL");
            preparedStatement = connection.prepareStatement(executeSQL);
            //String[] valueArr = values.split(",");
            String[] targetColsTypeArr = targetColsType.split(",");
            System.out.println("values " + values);
            System.out.println("targetColsType " + targetColsType);
            System.out.println("executeSQL " + executeSQL);


            for (int i = 0; i < values.size(); i++) {
                int paramIndex = i + 1;
                System.out.println("paramIndex :  " + paramIndex);
                System.out.println("targetColsTypeArr :  " + targetColsTypeArr[i]);
                if (targetColsTypeArr[i].equalsIgnoreCase(FiledTypeEnum.INTEGER.name())) {
                    preparedStatement.setInt(paramIndex, values.getInteger(i));
                } else if (targetColsTypeArr[i].equalsIgnoreCase(FiledTypeEnum.LONG.name())) {
                    preparedStatement.setLong(paramIndex, values.getLong(i));
                } else {
                    preparedStatement.setString(paramIndex, values.getString(i));
                }
            }
            int updateResult = preparedStatement.executeUpdate();
            System.out.println("执行结果:" + updateResult);
            //  boolean executeResult = preparedStatement.execute();
            //   System.out.println("执行结果:" + executeResult);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}