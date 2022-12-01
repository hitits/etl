package com.ffcs.etl.flink.source;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ffcs.etl.entity.pojo.mapping.Json2DbMapping;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @className: MysqlSource
 * @author: linzy
 * @date: 2022/11/29
 * 自定义MysqlSource
 */

public class MysqlSource extends RichSourceFunction<HashMap<String, JSONArray>> {
    private String host;
    private Integer port;
    private String db;
    private String user;
    private String passwd;
    private Integer secondInterval;
    private volatile boolean isRunning = true;
    private Connection connection;
    private PreparedStatement preparedStatement;

    public MysqlSource(String host, Integer port, String db, String user, String passwd, Integer secondInterval) {
        this.host = host;
        this.port = port;
        this.db = db;
        this.user = user;
        this.passwd = passwd;
        this.secondInterval = secondInterval;
    }

    /**
     * 开始时,在open()方法中建立连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + db + "?useUnicode=true&characterEncoding=utf8", user, passwd);
        String sql = "select id,targetTable,targetField,targetType,sourceField,sourceType  from Json_Db_Mapping";
        preparedStatement = connection.prepareStatement(sql);
    }

    /**
     * 执行完，调用close()方法关系连接，释放资源
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (preparedStatement != null) {
            preparedStatement.close();
        }
    }

    /**
     * 调用run()方法获取数据
     *
     * @param ctx
     */
    @Override
    public void run(SourceContext<HashMap<String, JSONArray>> ctx) {
        try {
            while (isRunning) {

                HashMap<String, JSONArray> output = new HashMap<>();
                ResultSet resultSet = preparedStatement.executeQuery();

                while (resultSet.next()) {
                    String id = resultSet.getString("id");
                    String targetTable = resultSet.getString("targetTable");
                    //  String value = resultSet.getString("value");
                    String value = "";
                    String targetField = resultSet.getString("targetField");
                    String targetType = resultSet.getString("targetType");
                    String sourceField = resultSet.getString("sourceField");
                    String sourceType = resultSet.getString("sourceType");
                    Json2DbMapping j2d = new Json2DbMapping(id, targetTable, value, targetField, targetType, sourceField, sourceType);
                //    System.out.println("查到的数据: " + j2d.toString());
                    if (!output.containsKey(targetTable)) {
                        JSONArray array = new JSONArray();
                        //array.add(JSONObject.parseObject(JSONObject.toJSONString(j2d)));
                        output.put(targetTable, array);
                    }
                    output.get(targetTable).add(j2d);
                }

                System.out.println("mysql 广播key: " + output.toString());
                ctx.collect(output);
                //每隔多少秒执行一次查询
                Thread.sleep(1000 * secondInterval);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 取消时，会调用此方法
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}