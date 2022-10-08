package com.atguigu.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DimApp {
    public static void main(String[] args) {
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


        //TODO 2.读取topic_db主题数据创建主流
        String topic = "topic_db";
        String groupId  = "dim_app_2126";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //TODO 3.过滤非json数据，保留新增变化以及初始化数据,转化为json格式
        //测输出可以看下脏数据在生产环境
       /* kafkaDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

                return false;
            }
        });*/
        SingleOutputStreamOperator<JSONObject> fliterJsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    //将数据转化为json格式
                    JSONObject jsonObject = JSON.parseObject(value);

                    //获取字段中的操作类型字段
                    String type = jsonObject.getString("type");

                    //保留新增，变化以及初始化数据
                    if ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type)) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("发现脏数据" + value);
                }
            }
        });

        //TODO 4.使用flinkCDC读取mysql配置信息表创建配置流
        //TODO 5.将配置流处理为广播流
        //TODO 6.连接主流与广播流
        //TODO 7.处理链接流根据配置信息处理主流数据
        //TODO 8.将数据写出到phoenix
        //TODO 9.启动任务
    }
}
