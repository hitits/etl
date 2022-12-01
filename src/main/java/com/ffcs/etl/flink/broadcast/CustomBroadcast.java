package com.ffcs.etl.flink.broadcast;

/**
 * @className: CustomBroadcast
 * @author: linzy
 * @date: 2022/11/29
 **/

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ffcs.etl.entity.enumerate.FiledTypeEnum;
import com.ffcs.etl.entity.pojo.mapping.Json2DbMapping;
import com.ffcs.etl.entity.pojo.pulsar.UserPulsar2Hudi;
import com.ffcs.etl.exception.DataException;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 自定义BroadcastProcessFunction
 * <p>
 * 当事件流中的用户ID在配置中出现时，才对该事件处理,并在事件中补全用户的基础信息
 * Tuple4<String,String,String,Integer>: 第一个流(事件流)的数据类型
 * HashMap<String,Tuple2<String,Integer>>: 第二个流(配置流)的数据类型
 * Tuple6<String,String,String,Integer,String,Integer>: 返回的数据类型
 */
public class CustomBroadcast extends BroadcastProcessFunction<String,
        HashMap<String, JSONArray>, JSONObject> {
    /**
     * 定义MapStateDescriptor
     */
    MapStateDescriptor<Void, Map<String, JSONArray>> configDescriptor = new MapStateDescriptor<>("config", Types.VOID,
            Types.MAP(Types.STRING, Types.GENERIC(JSONArray.class)));

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
    public void processElement(String value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //事件流中的用户ID

        try {
            JSONObject json = JSONObject.parseObject(value);

            //获取状态
            ReadOnlyBroadcastState<Void, Map<String, JSONArray>> broadcastState = ctx.getBroadcastState(configDescriptor);
            Map<String, JSONArray> broadcastStateUserInfo = broadcastState.get(null);

            //适配 json格式:
            JSONArray array = broadcastStateUserInfo.get(json.getString("table"));

            JSONObject executeSQL = createExecuteSQL(array, json);
            System.out.println("executeSQL : " + executeSQL);
            out.collect(executeSQL);

        } catch (Exception ex) {
            ex.printStackTrace();
        }


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
    public void processBroadcastElement(HashMap<String, JSONArray> value, Context ctx, Collector<JSONObject> out) throws Exception {
        //获取状态
        BroadcastState<Void, Map<String, JSONArray>> broadcastState = ctx.getBroadcastState(configDescriptor);
        //清空状态
        broadcastState.clear();
        //更新状态
        broadcastState.put(null, value);
    }

    public JSONObject createExecuteSQL(JSONArray array, JSONObject json) throws DataException {
        JSONObject data = json.getJSONObject("data");
        String fields = "";
     //   String values = "";
        JSONArray values = new JSONArray();
        String tablename = "";
        String questions = "";
        String targetColsType = "";
     //   System.out.println("array : " + array.toJSONString());
     //   System.out.println("json : " + json.toJSONString());
        for (int i = 0; i < array.size(); i++) {
            Json2DbMapping j2dm = JSONObject.toJavaObject(array.getJSONObject(i), Json2DbMapping.class);
            tablename = j2dm.getTargetTable();
            if (data.getString(j2dm.getSourceField()) == null) {
                continue;
            }
            values.add(data.getString(j2dm.getSourceField()));
          /*  if (j2dm.getTargetType().equalsIgnoreCase(FiledTypeEnum.STRING.name())) {
                values.add(data.getString(j2dm.getSourceField()));
                //values += String.format("%s,", data.getString(j2dm.getSourceField()));
            } else if (j2dm.getTargetType().equalsIgnoreCase(FiledTypeEnum.INTEGER.name())) {
                values.add(data.getString(j2dm.getSourceField()));
                values += String.format("%s,", data.getString(j2dm.getSourceField()));
            }*/
            fields += String.format("%s,", j2dm.getTargetField());
            questions += "?,";
            targetColsType += String.format("%s,", j2dm.getTargetType());


        }
     //   System.out.println("values : " + values);
     //   System.out.println("fields : " + fields);
        //删掉最后一个 ,
        //values = values.substring(0, values.length() - 1);
        fields = fields.substring(0, fields.length() - 1);
        questions = questions.substring(0, questions.length() - 1);

        JSONObject tedata = new JSONObject();
        if (values.size() != fields.split(",").length) {
            throw new DataException("字段不对应 values.size != fields.size");
        }
        //  System.out.println("createExecuteSQL : "+ tedata.toJSONString());
        tedata.put("executeSQL", String.format("insert into %s(%s) values(%s)", tablename, fields, questions));
        tedata.put("values", values);
/*        tedata.put("fields",fields);
        tedata.put("questions",questions);
        tedata.put("tablename",tablename);*/
        tedata.put("targetColsType", targetColsType);
        return tedata;
    }

}
