package com.atguigu.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

//数据流 web/app -> Nginx -> 日志服务器(.log) -> Flume -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkAPP -> kafka(DWD)
//程 序 Mock(lg.sh) -> flume(f1) -> kafka(zK) ->BaseLogApp ->kafka(Zk) ->DwdTrafficUserJumpDetail -> kafka(ZK)
/**
 * 用途共计后续跳出率，如果上一跳为null且后续没有其他页面就认为是跳出的
 */
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//生产环境中设置为kafka主题的分区数量

        /*//1.1开启checkpoint
        env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10*60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));

        //1.2设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020//211126/ck"); //因为要上传到集群，所以需要hadoop用户名
        System.setProperty("HADOOP_SUER_NAME","atguigu");*/
        //TODO 2.读取kafka 页面日志主题数据创建流
        String topic = "dwd_traffic_page_log";
        String groupId = "user_jump_detail_211126";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.将每行数据解析为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(line -> JSON.parseObject(line));

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS
            .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                        @Override
                        public long extractTimestamp(JSONObject element, long recordTimestamp) {
                            return element.getLong("ts");
                        }
                    }))
                .keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5.定义CEP的模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));
        //TODO 6.将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 7.提取时间（匹配上的事件以及超时事件）
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOut"){};

        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutTag, //是List集合是因为CEP有循环模式，而我们没用，只取第一个值就行
                new PatternTimeoutFunction<JSONObject, String>() {
                    @Override
                    public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0).toJSONString();
                    }
                },
                new PatternSelectFunction<JSONObject, String>() {
                    @Override
                    public String select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0).toJSONString();
                    }
                });

        DataStream<String> timeOutDS = selectDS.getSideOutput(timeOutTag);
        //TODO 8.合并两种事件
        DataStream<String> unionDS = selectDS.union(timeOutDS);
        //TODO 9.将数据写入kafka
        selectDS.print("Select>>>>>>>>>>>");
        timeOutDS.print("timeOut>>>>>>>>>");
        String targetTopic = "dwd_traffic_user_jump_detail";
        unionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));
        //TODO 10.启动任务
        env.execute("DwdTrafficUserJumpDetail");
    }
}
