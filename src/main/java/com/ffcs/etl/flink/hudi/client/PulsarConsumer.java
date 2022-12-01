package com.ffcs.etl.flink.hudi.client;

import com.alibaba.fastjson.JSONObject;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;

import java.util.concurrent.TimeUnit;

// 演示 Pulsar的消费者的使用  基于schema形式
public class PulsarConsumer {

    public static void main(String[] args) throws Exception {
        pulsarConsumerJSON();

    }
    public static void pulsarConsumerJSON() throws Exception{

        //1. 构建Pulsar的客户端对象
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://bigdata01:6650,bigdata02:6650,bigdata03:6650").build();

        //2. 通过客户端构建消费者对象

        Consumer<JSONObject> consumer = pulsarClient.newConsumer(JSONSchema.of(JSONObject.class))
                .topic("persistent://test_pulsar_tenant/test_pulsar_ns/topic_json")
                .subscriptionName("sub_04")
                // 设置支持批量读取参数配置
                .batchReceivePolicy(
                        BatchReceivePolicy.builder()
                                .maxNumBytes(1024 * 1024)
                                .maxNumMessages(100)
                                .timeout(2000, TimeUnit.MILLISECONDS)
                                .build()
                )
                .subscribe();

        //3. 循环读取数据
        while (true) {

            //3.1 读取消息(批量)
            Messages<JSONObject> messages = consumer.batchReceive();


            //3.2: 获取消息数据
            for (Message<JSONObject> message : messages) {
                JSONObject json = message.getValue();

                System.out.println("消息数据为: " +json.toJSONString());

                //3.3 ack确认
                consumer.acknowledge(message);
            }

        }
    }
    public static void pulsarConsumerAll() throws Exception{

        //1. 构建Pulsar的客户端对象
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://bigdata01:6650,bigdata02:6650,bigdata03:6650").build();

        //2. 通过客户端构建消费者对象

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://test_pulsar_tenant/test_pulsar_ns/topic_mis")
                .subscriptionName("sub_04")
                // 设置支持批量读取参数配置
                .batchReceivePolicy(
                        BatchReceivePolicy.builder()
                                .maxNumBytes(1024 * 1024)
                                .maxNumMessages(100)
                                .timeout(2000, TimeUnit.MILLISECONDS)
                                .build()
                )
                .subscribe();

        //3. 循环读取数据
        while (true) {

            //3.1 读取消息(批量)
            Messages<byte[]> messages = consumer.batchReceive();


            //3.2: 获取消息数据
            for (Message<byte[]> message : messages) {
                String msg = new String(message.getValue());

                System.out.println("消息数据为:"+msg);

                //3.3 ack确认
                consumer.acknowledge(message);
            }

        }
    }

    public static void pulsarConsumerBatchTest() throws Exception{

        //1. 构建Pulsar的客户端对象
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://bigdata01:6650,bigdata02:6650,bigdata03:6650").build();

        //2. 通过客户端构建消费者对象

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://test_pulsar_tenant/test_pulsar_ns/my_topic1")
                .subscriptionName("sub_04")
                // 设置支持批量读取参数配置
                .batchReceivePolicy(
                        BatchReceivePolicy.builder()
                                .maxNumBytes(1024 * 1024)
                                .maxNumMessages(100)
                                .timeout(2000, TimeUnit.MILLISECONDS)
                                .build()
                )
                .subscribe();

        //3. 循环读取数据
        while (true) {

            //3.1 读取消息(批量)
            Messages<String> messages = consumer.batchReceive();

            //3.2: 获取消息数据
            for (Message<String> message : messages) {
                String msg = message.getValue();

                System.out.println("消息数据为:"+msg);

                //3.3 ack确认
                consumer.acknowledge(message);
            }

        }
    }
  public static void pulsarConsumerTest()throws Exception{

        //1. 创建pulsar的客户端的对象
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://bigdata01:6650,bigdata02:6650,bigdata03:6650").build();

        //2. 基于客户端构建消费者对象

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("pulsar_test")
                .subscriptionName("sub_04")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        //3. 循环从消费者读取数据

        while(true) {
            //3.1: 接收消息
            Message<String> message = consumer.receive();

            //3.2: 获取消息
            String msg = message.getValue();

            //3.3: 处理数据--- 业务操作
            System.out.println("消息数据为:"+msg);

            //3.4: ack确认操作
            consumer.acknowledge(message);

            // 如果消费失败了, 可以采用try catch方式进行捕获异常, 捕获后, 可以进行告知没有消费
            //consumer.negativeAcknowledge(message);

        }
    }
}
