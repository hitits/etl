package com.ffcs.etl.flink.hudi.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ffcs.etl.entity.pojo.UserEventInfo;
import com.ffcs.etl.entity.pojo.hudi.UserHudi;
import com.ffcs.etl.entity.pojo.pulsar.UserPulsar2Hudi;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchema;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

// Pulsar整合flink, 完成让flink从Pulsar中读取消息写入hudi操作
@Slf4j
public class Pulsar2HudiJson {


    public static void main(String[] args) throws Exception {
        //1.配置信息
        String fromMysqlHost = "bigdata01";
        int fromMysqlPort = 3306;
        String fromMysqlDB = "test";
        String fromMysqlUser = "root";
        String fromMysqlPasswd = "ffcs@2022";
        int fromMysqlSecondInterval = 120;

        //2、配置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //3.1 创建广播流
        DataStreamSource<HashMap<String, UserPulsar2Hudi>> configStream = env.addSource(
                new MysqlSource(fromMysqlHost, fromMysqlPort, fromMysqlDB, fromMysqlUser, fromMysqlPasswd, fromMysqlSecondInterval));
        MapStateDescriptor<Void, Map<String, UserPulsar2Hudi>> configDescriptor = new MapStateDescriptor<>("config", Types.VOID,
                Types.MAP(Types.STRING, Types.POJO(UserPulsar2Hudi.class)));
        /* 3.2 将配置信息广播，broadcastConfigStream */
        BroadcastStream<HashMap<String, UserPulsar2Hudi>> broadcastConfigStream = configStream.broadcast(configDescriptor);

        //4. 添加source数据源, 用于读取数据 : Pulsar
        Properties props = new Properties();
        props.setProperty("topic", "persistent://test_pulsar_tenant/test_pulsar_ns/test_pulsar_user_json2");
        FlinkPulsarSource<String> pulsarSource = new FlinkPulsarSource<String>(
                "pulsar://bigdata01:6650,bigdata02:6650,bigdata03:6650",
                "http://bigdata01:12080,bigdata02:12080,bigdata03:12080",
                PulsarDeserializationSchema.valueOnly(new SimpleStringSchema()),
                props
        );
        pulsarSource.setStartFromLatest();
        DataStreamSource<String> streamSource = env.addSource(pulsarSource);

        //5、事件流和广播的配置流连接，形成BroadcastConnectedStream
        BroadcastConnectedStream<String, HashMap<String, UserPulsar2Hudi>> connectedStream = streamSource.connect(broadcastConfigStream);

        //6、对BroadcastConnectedStream应用process方法，根据配置(规则)处理事件
        DataStream<UserHudi> resultStream = connectedStream.process(new CustomBroadcastProcessFunction());

        //7.写入hudi
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()//设置流式模式
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        //7.1转为table
        Table table = tableEnv.fromDataStream(resultStream);
        tableEnv.createTemporaryView("test_pulsar_user_json", table);
        tableEnv.executeSql(
                " CREATE TABLE test_hudi_user_json(\n" +
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
                        "                        )\n" +
                        "                        PARTITIONED BY (partitionDay)\n" +
                        "                        WITH(\n" +
                        "                            'connector' = 'hudi',\n" +
                        "                            'path'='hdfs://ffcluster:8020/hudi-warehouse/test_hudi_user_json',\n" +
                        "                            'table.type' = 'COPY_ON_WRITE',\n" +
                        "                            'hive_sync.enable'='true',\n" +
                        "                            'hive_sync.table'='test_hudi_user_json', \n" +
                        "                            'hive_sync.db'='hudi_dbs', \n" +
                        "                            'hive_sync.mode' = 'hms',\n" +
                        "                            'hive_sync.metastore.uris' = 'thrift://bigdata01:9083', " +
                        "                            'write.operation' = 'upsert',\n" +
                        "                            'hoodie.datasource.write.recordkey.field' = 'userID',\n" +
                        "                            'write.precombine.field' = 'ts',\n" +
                        "                            'write.tasks' = '1'\n" +
                        "                        )"
        );

        //5.通过子查询的方式，将数据写入输出表
        tableEnv.executeSql(
                "INSERT INTO test_hudi_user_json " +
                        "SELECT userId, name, createTime, address, age,sex,  TO_TIMESTAMP(createTime,'yyyy-MM-dd HH:mm:ss') ts ,image, partitionDay FROM test_pulsar_user_json"
        );




        //7.2.写入hudi
/*        DataStream<RowData> dataStream = tableEnv.toAppendStream(table, RowData.class);
        String targetTable = "hudiSinkTable";
        String basePath = "hdfs://ffcluster:8020/hudi-warehouse/test_hudi_user_json";

        Map<String, String> options = new HashMap<>();
        options.put(FlinkOptions.PATH.key(), basePath);
        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
        options.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts");
        options.put(FlinkOptions.INDEX_BOOTSTRAP_ENABLED.key(), "true");

        HoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)
                .column("userId VARCHAR(50)")
                .column("name VARCHAR(50)")
                .column("createTime VARCHAR(20)")
                .column("address VARCHAR(50)")
                .column("age INT")
                .column("sex INT")
                .column("image BINARY")
                .column("ts TIMESTAMP(3)")
                .column("partitionDay VARCHAR(20)")
                .pk("userId")
                .partition("partitionDay")
                .options(options);
// The second parameter indicating whether the input data stream is bounded
        builder.sink(dataStream, false);*/

        env.execute("Api_Sink");

    }

    static class MyPulsarSink extends RichSinkFunction<UserHudi> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void invoke(UserHudi value, Context context) throws Exception {

        }
    }


    /**
     * 自定义BroadcastProcessFunction
     * <p>
     * 当事件流中的用户ID在配置中出现时，才对该事件处理,并在事件中补全用户的基础信息
     * Tuple4<String,String,String,Integer>: 第一个流(事件流)的数据类型
     * HashMap<String,Tuple2<String,Integer>>: 第二个流(配置流)的数据类型
     * Tuple6<String,String,String,Integer,String,Integer>: 返回的数据类型
     */
    static class CustomBroadcastProcessFunction extends BroadcastProcessFunction<String,
            HashMap<String, UserPulsar2Hudi>, UserHudi> {
        /**
         * 定义MapStateDescriptor
         */
        MapStateDescriptor<Void, Map<String, UserPulsar2Hudi>> configDescriptor = new MapStateDescriptor<>("config", Types.VOID,
                Types.MAP(Types.STRING, Types.POJO(UserPulsar2Hudi.class)));

        /**
         * 读取状态，并基于状态，处理事件流中的数据
         * 在这里，从上下文中获取状态，基于获取的状态，对事件流中的数据进行处理
         *
         * @param value 事件流中的数据
         * @param ctx   上下文
         * @param out   输出零条或多条数据
         * @throws Exception
         */
        @Override
        public void processElement(String value, ReadOnlyContext ctx, Collector<UserHudi> out) throws Exception {
            //事件流中的用户ID

            try {
                JSONObject json = JSONObject.parseObject(value);

                //获取状态
                ReadOnlyBroadcastState<Void, Map<String, UserPulsar2Hudi>> broadcastState = ctx.getBroadcastState(configDescriptor);
                Map<String, UserPulsar2Hudi> broadcastStateUserInfo = broadcastState.get(null);
                //适配 json格式:
                UserPulsar2Hudi uph = broadcastStateUserInfo.get(json.getString("sourceCategory"));

                System.out.println("sourceCategory :" + uph);
                System.out.println("json : " + json.toJSONString());
                if (uph != null) {
                    UserHudi uh = new UserHudi();
                    uh.setUserId(json.getString(uph.getUserId()));
                    uh.setAddress(json.getString(uph.getAddress()));
                    uh.setAge(json.getInteger(uph.getAge()));
                    uh.setCreateTime(json.getString(uph.getCreateTime()));
                    uh.setName(json.getString(uph.getName()));
                    uh.setSex(json.getIntValue(uph.getSex()));
                    uh.setImage(json.getString(uph.getImage()).getBytes());
                    String partitionDay = uh.getCreateTime().substring(0, 10);
                    uh.setPartitionDay(partitionDay);
                    out.collect(uh);
                } else {
                    UserHudi uh = new UserHudi();
                    uh.setUserId("user_err" + System.currentTimeMillis());
                    uh.setAddress("none");
                    uh.setAge(1);
                    uh.setCreateTime("2022-11-11 11:11:11");
                    uh.setName("none");
                    uh.setSex(0);
                    uh.setImage("none".getBytes());
                    String partitionDay = uh.getCreateTime().substring(0, 10);
                    uh.setPartitionDay(partitionDay);
                    out.collect(uh);
                }

            } catch (Exception ex) {
                ex.printStackTrace();
            }

            //配置中有此用户，则在该事件中添加用户的userName、userAge字段。
            // 配置中没有此用户，则丢弃
       /*     Tuple2<String, Integer> userInfo = broadcastStateUserInfo.get(userID);
            if (userInfo != null) {
                out.collect(new Tuple6<>(value.f0, value.f1, value.f2, value.f3, userInfo.f0, userInfo.f1));
            }*/
        }

        /**
         * 处理广播流中的每一条数据，并更新状态
         *
         * @throws Exception
         * @paramvalue 广播流中的数据
         * @paramctx 上下文
         * @paramout 输出零条或多条数据
         */
        @Override
        public void processBroadcastElement(HashMap<String, UserPulsar2Hudi> value, Context ctx, Collector<UserHudi> out) throws Exception {
            //获取状态
            BroadcastState<Void, Map<String, UserPulsar2Hudi>> broadcastState = ctx.getBroadcastState(configDescriptor);
            //清空状态
            broadcastState.clear();
            //更新状态
            broadcastState.put(null, value);
        }
    }


    /**
     * 自定义MysqlSource，每隔secondInterval秒从Mysql中获取一次配置
     */
    static class MysqlSource extends RichSourceFunction<HashMap<String, UserPulsar2Hudi>> {
        private String host;
        private Integer port;
        private String db;
        private String user;
        private String passwd;
        private Integer secondInterval;
        private volatile boolean isRunning = true;
        private Connection connection;
        private PreparedStatement preparedStatement;

        MysqlSource(String host, Integer port, String db, String user, String passwd, Integer secondInterval) {
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
            String sql = "select sourceCategory,userId,name,createTime,address,age,sex,image,partitionDay from User_Pulsar_Hudi";
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
        public void run(SourceContext<HashMap<String, UserPulsar2Hudi>> ctx) {
            try {
                while (isRunning) {
                    HashMap<String, UserPulsar2Hudi> output = new HashMap<>();
                    ResultSet resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        String sourceCategory = resultSet.getString("sourceCategory");
                        String userId = resultSet.getString("userId");
                        String name = resultSet.getString("name");
                        String createTime = resultSet.getString("createTime");
                        String address = resultSet.getString("address");
                        String age = resultSet.getString("age");
                        String sex = resultSet.getString("sex");
                        String image = resultSet.getString("image");
                        String partitionDay = resultSet.getString("partitionDay");
                        UserPulsar2Hudi up2h = new UserPulsar2Hudi(sourceCategory, userId, name, createTime, address, age, sex, image, partitionDay);
                        output.put(sourceCategory, up2h);
                    }
                    ctx.collect(output);
                    //每隔多少秒执行一次查询
                    Thread.sleep(1000 * secondInterval);
                }
            } catch (Exception ex) {
                log.error("从Mysql获取配置异常...", ex);
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

}
