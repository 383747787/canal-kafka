package com.caiyunworks.search;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.net.InetSocketAddress;
import java.util.Properties;

/**
 * 单机模式的测试例子
 *
 * @author jianghang 2013-4-15 下午04:19:20
 * @version 1.0.4
 */
public class SimpleCanalClient extends AbstractCanalClient {

    public SimpleCanalClient(String destination) {
        super(destination);
    }

    public static void main(String args[]) {
        // 根据ip，直接创建链接，无HA的功能
        String destination = "example";
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(
                "127.0.0.1", 11111), destination, "", "");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        final SimpleCanalClient clientTest = new SimpleCanalClient(destination);
        clientTest.setConnector(connector);
        clientTest.setKafkaProducer(producer);
        clientTest.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                try {
                    logger.info("## stop the canal client");
                    clientTest.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e));
                } finally {
                    logger.info("## canal client is down.");
                }
            }

        });
    }

}
