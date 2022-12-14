package com.ffcs.etl.flink.doris.table;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ffcs.etl.entity.pojo.hudi.UserHudi;
import com.ffcs.etl.entity.pojo.mapping.Json2DbMapping;
import com.ffcs.etl.entity.pojo.pulsar.UserPulsar2Hudi;
import com.ffcs.etl.flink.broadcast.CustomBroadcast;
import com.ffcs.etl.flink.sink.MyDorisSink;
import com.ffcs.etl.flink.source.MysqlSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

// Pulsar??????flink, ?????????flink???Pulsar?????????????????????doris??????
@Slf4j
public class Pulsar2DorisTable {


    public static void main(String[] args) throws Exception {
        //1.????????????
        String fromMysqlHost = "bigdata01";
        int fromMysqlPort = 3306;
        String fromMysqlDB = "test";
        String fromMysqlUser = "root";
        String fromMysqlPasswd = "ffcs@2022";
        int fromMysqlSecondInterval = 120;

        String dorisHost = "bigdata02";
        int dorisPort = 9030;
        String dorisDB = "test_db";
        String dorisUser = "root";
        String dorisPasswd = "ffcs@2022";
        int dorisSecondInterval = 120;

        String topic = "persistent://test_pulsar_tenant/test_pulsar_ns/test_json_xq";
        String serviceUrl = "pulsar://bigdata01:6650,bigdata02:6650,bigdata03:6650";
        String adminUrl = "http://bigdata01:12080,bigdata02:12080,bigdata03:12080";
        //2?????????????????????
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //3.1 ???????????????
        DataStreamSource<HashMap<String, JSONArray>> configStream = env.addSource(
                new MysqlSource(fromMysqlHost, fromMysqlPort, fromMysqlDB, fromMysqlUser, fromMysqlPasswd, fromMysqlSecondInterval));
        MapStateDescriptor<Void, Map<String, JSONArray>> configDescriptor = new MapStateDescriptor<>("config", Types.VOID,
                Types.MAP(Types.STRING,Types.GENERIC(JSONArray.class)));
        /* 3.2 ????????????????????????broadcastConfigStream */
        BroadcastStream<HashMap<String, JSONArray>> broadcastConfigStream = configStream.broadcast(configDescriptor);

        //4. ??????source?????????, ?????????????????? : Pulsar
        Properties props = new Properties();
        props.setProperty("topic", topic);
        FlinkPulsarSource<String> pulsarSource = new FlinkPulsarSource<String>(
                serviceUrl,
                adminUrl,
                PulsarDeserializationSchema.valueOnly(new SimpleStringSchema()),
                props
        );
        pulsarSource.setStartFromEarliest();
        DataStreamSource<String> streamSource = env.addSource(pulsarSource);

        //5????????????????????????????????????????????????BroadcastConnectedStream
        BroadcastConnectedStream<String, HashMap<String, JSONArray>> connectedStream = streamSource.connect(broadcastConfigStream);

        //6??????BroadcastConnectedStream??????process?????????????????????(??????)????????????
        DataStream<JSONObject> resultStream = connectedStream.process(new CustomBroadcast());

        resultStream.addSink(new MyDorisSink(dorisHost, dorisPort, dorisDB, dorisUser, dorisPasswd, dorisSecondInterval)).name("doris db");
        env.execute("doris_table");

    }






}
