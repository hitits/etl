package com.ffcs.etl.flink.hudi.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ffcs.etl.entity.pojo.EventInfo;
import com.ffcs.etl.entity.pojo.UserInfo;
import com.ffcs.etl.entity.pojo.pulsar.UserPulsar2Hudi;
import com.ffcs.etl.entity.pojo.pulsar.UserPulsarType1;
import com.ffcs.etl.entity.pojo.pulsar.UserPulsarType2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.util.serialization.PulsarPrimitiveSchema;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;

import java.io.*;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Optional;
import java.util.Properties;

public class PulsarProducer {


    public static void main(String[] args) throws Exception {

       createDataType1();
        createDataType2();
    }

    public static void createDataType2() throws Exception{
        FileInputStream is =null;
        // InputStreamReader isr =null;
        try {
            File file = new File("D:\\temp\\1.jpg");
            is = new FileInputStream(file);
            //   isr = new InputStreamReader(is);
            // inputStream2Base64(is);
            SecureRandom rd = new SecureRandom();
            //pulsarProducerUSERJSONTest();
            UserPulsarType2 up = new UserPulsarType2();
            up.setSourceCategory("json_type2");
            up.setuAddr("nj");
            up.setuAge(32);
            up.setCreateTime("2022-07-12 08:00:00");
            up.setuName("jim");
            up.setuSex(0);
            up.setuId("user008");
            up.setuImage(inputStream2Base64(is));
            String topic = "persistent://test_pulsar_tenant/test_pulsar_ns/test_pulsar_user_json2";

            pulsarProducerObject(up,topic);
        }catch (Exception ex ){
            ex.printStackTrace();
        }finally {
         /*   if(isr !=null){
                isr.close();
            }*/
            if(is !=null){
                is.close();
            }

        }
    }
    public static void createDataType1() throws Exception{
        FileInputStream is =null;
        // InputStreamReader isr =null;
        try {
            File file = new File("D:\\temp\\2.jpg");
            is = new FileInputStream(file);
            //   isr = new InputStreamReader(is);
            // inputStream2Base64(is);
            SecureRandom rd = new SecureRandom();
            //pulsarProducerUSERJSONTest();
            UserPulsarType1 up = new UserPulsarType1();
            up.setSourceCategory("json_type1");
            up.setAddress("bj");
            up.setAge(22);
            up.setCreateTime("2022-08-12 08:00:00");
            up.setName("jack");
            up.setSex(1);
            up.setUserId("user007");
            up.setImage(inputStream2Base64(is));
            String topic = "persistent://test_pulsar_tenant/test_pulsar_ns/test_pulsar_user_json2";

            pulsarProducerObject(up,topic);
        }catch (Exception ex ){
            ex.printStackTrace();
        }finally {
         /*   if(isr !=null){
                isr.close();
            }*/
            if(is !=null){
                is.close();
            }

        }
    }

    public static void pulsarProducerObject(Object up, String topic) throws PulsarClientException {
        //1. 创建pulsar的客户端对象
        //  {"userID":"user_3","eventTime":"2019-08-17 12:19:47","eventType":"browse","productID":1}

        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://bigdata01:6650,bigdata02:6650,bigdata03:6650").build();
         //2. 基于客户端对象进行构建生产者对象
        Producer<JSONObject> producer = pulsarClient.newProducer(JSONSchema.of(JSONObject.class))
                .topic(topic)
                .create();
        System.out.println("发送数据:");
        System.out.println(JSONObject.toJSONString(up));
        producer.send(JSONObject.parseObject(JSONObject.toJSONString(up)));

        //4. 释放资源
        producer.close();
        pulsarClient.close();
    }

    public static void pulsarProducerUSERJSONTest() throws PulsarClientException {
        //1. 创建pulsar的客户端对象
      //  {"userID":"user_3","eventTime":"2019-08-17 12:19:47","eventType":"browse","productID":1}

        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://bigdata01:6650,bigdata02:6650,bigdata03:6650").build();

        //2. 基于客户端对象进行构建生产者对象
        Producer<JSONObject> producer = pulsarClient.newProducer(JSONSchema.of(JSONObject.class))
                .topic("persistent://test_pulsar_tenant/test_pulsar_ns/test_pulsar_user")
                .create();
        SecureRandom rd = new SecureRandom();
        for(int i =0;i<1;i++) {
            //3. 进行数据生产
            EventInfo ei = new EventInfo();
            ei.setEventTime("2020-12-17 10:19:47");
            ei.setUserID("user_6");
            ei.setEventType("click");
            ei.setProductID(12);
            producer.send(JSONObject.parseObject(JSONObject.toJSONString(ei)));
        }
        //4. 释放资源
        producer.close();
        pulsarClient.close();
    }

  public static void pulsarProducerAsyncTest() throws InterruptedException, PulsarClientException {

        //1. 创建Pulsar的客户端对象
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://bigdata01:6650,bigdata02:6650,bigdata03:6650").build();

        //2. 通过客户端构建生产者的对象
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://test_pulsar_tenant/test_pulsar_ns/topic_mis")
                .create();
        //3. 进行数据发送操作
        // 发现数据并没有生产成功, 主要原因是
        //          因为采用异步的发送方案, 这种发送方案会先将数据写入到客户端缓存中, 当缓存中数据达到一批后 才会进行发送操作
        producer.sendAsync("hello async pulsar...2222");
        System.out.println("数据生产成功....");

        // 可以发送完成后, 让程序等待一下, 让其将缓冲区中数据刷新到pulsar上 然后在结束
        Thread.sleep(1000);
        //4. 释放资源
        producer.close();
        pulsarClient.close();
    }



    public static void pulsarProducerSyncTest()throws Exception {

            //1. 创建Pulsar的客户端对象
            PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://bigdata01:6650,bigdata02:6650,bigdata03:6650").build();

            //2. 通过客户端创建生产者的对象

            Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                    .topic("persistent://test_pulsar_tenant/test_pulsar_ns/pulsar_test")
                    .create();
            //3. 使用生产者发送数据
            producer.send("hello java API pulsar ...2");

            System.out.println("数据生产完成....");
            //4. 释放资源
            producer.close();
            pulsarClient.close();


        }
    /**
     * 将inputstream转为Base64
     *
     * @param is
     * @return
     * @throws Exception
     */
    private static String inputStream2Base64(InputStream is) throws Exception {
        byte[] data = null;
        try {
            ByteArrayOutputStream swapStream = new ByteArrayOutputStream();
            byte[] buff = new byte[100];
            int rc = 0;
            while ((rc = is.read(buff, 0, 100)) > 0) {
                swapStream.write(buff, 0, rc);
            }
            data = swapStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    throw new Exception("输入流关闭异常");
                }
            }
        }

        return Base64.getEncoder().encodeToString(data);
    }

}
