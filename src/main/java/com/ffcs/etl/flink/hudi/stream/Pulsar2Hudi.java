package com.ffcs.etl.flink.hudi.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ffcs.etl.entity.pojo.UserEventInfo;
import com.mysql.jdbc.Driver;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchema;
import org.apache.flink.streaming.util.serialization.PulsarPrimitiveSchema;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.TimeUnit;

// Pulsar??????flink, ?????????flink???Pulsar????????????????????????
@Slf4j
public class Pulsar2Hudi {

    public static void main(String[] args) throws Exception {
        //???????????????
        String fromMysqlHost ="bigdata01";
        int fromMysqlPort = 3306;
        String fromMysqlDB = "test";
        String fromMysqlUser = "root";
        String fromMysqlPasswd = "ffcs@2022";
        int fromMysqlSecondInterval = 3600;

        //2?????????????????????
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //  StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());


        //2. ??????source?????????, ?????????????????? : Pulsar
        Properties props = new Properties();
        props.setProperty("topic","persistent://test_pulsar_tenant/test_pulsar_ns/test_pulsar_user");

        FlinkPulsarSource<String> pulsarSource = new FlinkPulsarSource<String>(
                "pulsar://bigdata01:6650,bigdata02:6650,bigdata03:6650",
                "http://bigdata01:12080,bigdata02:12080,bigdata03:12080",
                PulsarDeserializationSchema.valueOnly(new SimpleStringSchema()),
                props
        );
        pulsarSource.setStartFromLatest();

        DataStreamSource<String> streamSource = env.addSource(pulsarSource);




        //??????StateBackend
        //    env.setStateBackend((StateBackend) new FsStateBackend(checkpointDirectory, true));
        //??????Checkpoint
/*        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(checkpointSecondInterval * 1000);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        */


        SingleOutputStreamOperator<Tuple4<String, String, String, Integer>> eventStream = streamSource.process(
                new ProcessFunction<String, Tuple4<String, String, String, Integer>>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<Tuple4<String, String, String, Integer>> out) {
                        try {
                            JSONObject obj = JSON.parseObject(value);
                            String userID = obj.getString("userID");
                            String eventTime = obj.getString("eventTime");
                            String eventType = obj.getString("eventType");
                            int productID = obj.getIntValue("productID");
                            out.collect(new Tuple4<>(userID, eventTime, eventType, productID));
                        } catch (Exception ex) {
                            log.warn("????????????:{}", value, ex);
                        }
                    }
                });
        //4???Mysql?????????
        // ?????????MysqlSource??????????????????Mysql?????????????????????????????????
        // ??????:??????ID,???????????????????????????
        DataStreamSource<HashMap<String, Tuple2<String, Integer>>> configStream = env.addSource(
                new  MysqlSource(fromMysqlHost, fromMysqlPort, fromMysqlDB, fromMysqlUser, fromMysqlPasswd, fromMysqlSecondInterval));
        /* (1)?????????MapStateDescriptor MapStateDescriptor???????????????????????????Key???Value????????????
         ?????????MapStateDescriptor??????key???Void?????????value???Map<String, Tuple2<String,Int>>????????? */

        MapStateDescriptor<Void, Map<String, Tuple2<String, Integer>>> configDescriptor = new MapStateDescriptor<>("config", Types.VOID,
                Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));
        /* (2)???????????????????????????BroadcastStream */
        BroadcastStream<HashMap<String, Tuple2<String, Integer>>> broadcastConfigStream = configStream.broadcast(configDescriptor);
        //5????????????????????????????????????????????????BroadcastConnectedStream
        BroadcastConnectedStream<Tuple4<String, String, String, Integer>, HashMap<String, Tuple2<String, Integer>>> connectedStream = eventStream.connect(broadcastConfigStream);
        //6??????BroadcastConnectedStream??????process?????????????????????(??????)????????????
        SingleOutputStreamOperator<Tuple6<String, String, String, Integer, String, Integer>> resultStream = connectedStream.process(new  CustomBroadcastProcessFunction());
        //7???????????????
      //  resultStream.print();
/*        SingleOutputStreamOperator<JSONObject> map = resultStream.map(new MapFunction<Tuple6<String, String, String, Integer, String, Integer>, JSONObject>() {
            @Override
            public JSONObject map(Tuple6<String, String, String, Integer, String, Integer> t6) throws Exception {

                return null;
            }
        });*/

       //env.fromCollection(map);
        DataStream<UserEventInfo> ds = resultStream.map(new MapFunction<Tuple6<String, String, String, Integer, String, Integer>, UserEventInfo>() {
            @Override
            public UserEventInfo map(Tuple6<String, String, String, Integer, String, Integer> t6) throws Exception {
                String userID = t6.f0;
                String eventTime = t6.f1;
                String eventType = t6.f2;
                int productID = t6.f3;
                String userName =t6.f4;
                int userAge  =t6.f5;
                String partitionDay = eventTime.substring(0, 10);
                UserEventInfo uei = new UserEventInfo(userID,userName,userAge,eventTime,eventType,productID,partitionDay);
                return uei;
            }
        });


        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()//??????????????????
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        Table table = tableEnv.fromDataStream(ds);

        tableEnv.createTemporaryView("pulsar_user_event_info",table);
        //4.??????hudi



        tableEnv.executeSql(
                " CREATE TABLE test_user_event_info(\n" +
                        "                            `userID` STRING,\n" +
                        "                            `userName` STRING,\n" +
                        "                            `eventTime` STRING,\n" +
                        "                            `userAge` INT,\n" +
                        "                            `productID` INT,\n" +
                        "                            `ts`  TIMESTAMP(3),\n" +
                        "                            `eventType` STRING,\n" +
                        "                            `partitionDay` STRING,\n" +
                        "                             primary key (userID) not enforced \n" +
                        "                        )\n" +
                        "                        PARTITIONED BY (partitionDay)\n" +
                        "                        WITH(\n" +
                        "                            'connector' = 'hudi',\n" +
                        "                            'path'='hdfs://ffcluster:8020/hudi-warehouse/test_user_event_info_pulsar2hudi',\n" +
                        "                            'table.type' = 'COPY_ON_WRITE',\n" +
                        "                            'hive_sync.enable'='true',\n" +
                        "                            'hive_sync.table'='test_user_event_info_pulsar2hudi', \n" +
                        "                            'hive_sync.db'='hudi_dbs', \n" +
                        "                            'hive_sync.mode' = 'hms',\n" +
                        "                            'hive_sync.metastore.uris' = 'thrift://bigdata01:9083', " +
                        "                            'write.operation' = 'upsert',\n" +
                        "                            'hoodie.datasource.write.recordkey.field' = 'userID',\n" +
                        "                            'write.precombine.field' = 'ts',\n" +
                        "                            'write.tasks' = '1'\n" +
                        "                        )"
        );

        //5.???????????????????????????????????????????????????
        tableEnv.executeSql(
                "INSERT INTO test_user_event_info " +
                        "SELECT userID, userName, eventTime, userAge, productID,  TO_TIMESTAMP(eventTime,'yyyy-MM-dd HH:mm:ss') ts ,eventType, partitionDay FROM pulsar_user_event_info"
        );

        // 8?????????JobGraph??????????????????
        env.execute();
    }




    /**
     * ?????????BroadcastProcessFunction
     * <p>
     * ????????????????????????ID?????????????????????????????????????????????,??????????????????????????????????????????
     * Tuple4<String,String,String,Integer>: ????????????(?????????)???????????????
     * HashMap<String,Tuple2<String,Integer>>: ????????????(?????????)???????????????
     * Tuple6<String,String,String,Integer,String,Integer>: ?????????????????????
     */
    static class CustomBroadcastProcessFunction extends BroadcastProcessFunction<Tuple4<String, String, String, Integer>,
            HashMap<String, Tuple2<String, Integer>>, Tuple6<String, String, String, Integer, String, Integer>> {
        /**
         * ??????MapStateDescriptor
         */
        MapStateDescriptor<Void, Map<String, Tuple2<String, Integer>>> configDescriptor = new MapStateDescriptor<>("config", Types.VOID,
                Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));

        /**
         * ????????????????????????????????????????????????????????????
         * ??????????????????????????????????????????????????????????????????????????????????????????????????????
         *
         * @paramvalue ?????????????????????
         * @paramctx ?????????
         * @paramout ???????????????????????????
         * @throws Exception
         */
        @Override
        public void processElement(Tuple4<String, String, String, Integer> value, ReadOnlyContext ctx, Collector<Tuple6<String, String, String, Integer, String, Integer>> out) throws Exception {
            //?????????????????????ID
            String userID = value.f0;
            //????????????
            ReadOnlyBroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(configDescriptor);
            Map<String, Tuple2<String, Integer>> broadcastStateUserInfo = broadcastState.get(null);
            //?????????????????????????????????????????????????????????userName???userAge?????????
            // ????????????????????????????????????
            Tuple2<String, Integer> userInfo = broadcastStateUserInfo.get(userID);
            if (userInfo != null) {
                out.collect(new Tuple6<>(value.f0, value.f1, value.f2, value.f3, userInfo.f0, userInfo.f1));
            }
        }

        /**
         * ??????????????????????????????????????????????????????
         * @paramvalue ?????????????????????
         * @paramctx ?????????
         * @paramout ???????????????????????????
         * @throws Exception
         */
        @Override
        public void processBroadcastElement(HashMap<String, Tuple2<String, Integer>> value, Context ctx, Collector<Tuple6<String, String, String, Integer, String, Integer>> out) throws Exception {
            //????????????
            BroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(configDescriptor);
            //????????????
            broadcastState.clear();
            //????????????
            broadcastState.put(null, value);
        }
    }

    /**
     * ?????????MysqlSource?????????secondInterval??????Mysql?????????????????????
     */
    static class MysqlSource extends RichSourceFunction<HashMap<String, Tuple2<String, Integer>>> {
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
         * ?????????,???open()?????????????????????
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + db + "?useUnicode=true&characterEncoding=utf8", user, passwd);
            String sql = "select userID,userName,userAge from user_info";
            preparedStatement = connection.prepareStatement(sql);
        }

        /**
         * ??????????????????close()?????????????????????????????????
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
         * ??????run()??????????????????
         * @param ctx
         */
        @Override
        public void run(SourceContext<HashMap<String, Tuple2<String, Integer>>> ctx) {
            try {
                while (isRunning) {
                    HashMap<String, Tuple2<String, Integer>> output = new HashMap<>();
                    ResultSet resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        String userID = resultSet.getString("userID");
                        String userName = resultSet.getString("userName");
                        int userAge = resultSet.getInt("userAge");
                        output.put(userID, new Tuple2<>(userName, userAge));
                    }
                    ctx.collect(output);
                    //?????????????????????????????????
                    Thread.sleep(1000 * secondInterval);
                }
            } catch (Exception ex) {
                log.error("???Mysql??????????????????...", ex);
            }
        }

        /**
         * ??????????????????????????????
         */
        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
