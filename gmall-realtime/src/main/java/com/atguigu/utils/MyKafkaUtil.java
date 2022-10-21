package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.function.Consumer;

public class MyKafkaUtil {

    private static final String KAFKA_SERVER="hadoop102:9092";
    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic,String groupId) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);

        return new FlinkKafkaConsumer<String>(
                topic,
                new KafkaDeserializationSchema<String>() { //反序列化
                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                        if(consumerRecord==null || consumerRecord.value()==null){
                            return null;
                        }else {
                            return new String(consumerRecord.value());
                        }
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                },//反序列化
                properties
        );
    }

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic){

        return new FlinkKafkaProducer<String>(KAFKA_SERVER,
        topic,
        new SimpleStringSchema());
    }
    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic,String defautTopic){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        return new FlinkKafkaProducer<String>(defautTopic,
                new KafkaSerializationSchema<String>() {  //自定义序列化器
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        if(element==null){
                            return new ProducerRecord<>(topic,"".getBytes());
                        }
                     return new ProducerRecord<>(topic,element.getBytes());
                    }
                },properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    /**
     * Kafka-Source DDL(Data Definition Language) 语句
     *
     * @param topic   数据源主题
     * @param groupId 消费者组
     * @return 拼接好的 Kafka 数据源 DDL 语句
     */
    public static String getKafkaDDL(String topic, String groupId) {

        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'group-offsets')";
    }
    /**
     * Kafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 Kafka-Sink DDL 语句
     */
    public static String getKafkaSinkDDL(String topic) {
        return "WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                "  'format' = 'json' " +
                ")";
    }


    /**
     *  topic_db 主题的 kafka-Source DDL 语句
     * @param groupId 消费者组
     * @return        拼接好的 Kafka 数据源的DDL语句
     */
    public static String getTopicDb(String groupId){
        return "CREATE TABLE topic_db (   " +
                "  `database` STRING,   " +
                "  `table` STRING,   " +
                "  `type` STRING,   " +
                "  `data` MAP<STRING,STRING>,   " +
                "  `old` MAP<STRING,STRING>,   " +
                "  `pt` AS PROCTIME()   " +
                ")" + getKafkaDDL("topic_db",groupId);
    }

    /**
     * UpsertKafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 UpsertKafka-Sink DDL 语句
     */
    public static String getUpsertKafkaDDL(String topic) {

        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }

}
