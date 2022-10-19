package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//数据流 web/app -> Nginx -> 日志服务器(.log) -> Flume -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkAPP -> kafka(DWD)
//程 序 Mock(lg.sh) -> flume(f1) -> kafka(zK) ->BaseLogApp ->kafka(Zk) ->DwdTrafficUniqueVisitorDetail -> kafka(ZK)

/**
 * 过滤每天的单独用户，用于后续统计每天的用户数量--独立访客
 */
public class DwdTrafficUniqueVisitorDetail {
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
        //TODO 2.读取kafka 页面日志主题创建流
        String topic = "dwd_traffic_page_log";
        String groupId = "unique_visitor_detail_211126";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.过滤掉上一跳页面不为null的数据并将每行数据转化为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    //获取上一跳页面id
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageId == null) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(value);
                }
            }
        });
        //TODO 4.按照Mid分组，每个Mid就有自己的一个状态了
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        //TODO 5.使用状态编程实现按照Mid的去重
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);
                //设置状态的过期时间TTL
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                lastVisitState = getRuntimeContext().getState(stateDescriptor);
            }
            @Override
            public boolean filter(JSONObject value) throws Exception {
                //获取状态数据&当前数据中的时间戳并转化为日期
                String lastDate = lastVisitState.value();
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);

                if (lastDate == null || !lastDate.equals(curDate)) {
                    lastVisitState.update(curDate);
                    return true;
                } else {
                    return false;
                }
            }
        });
        //TODO 6.将数据写到kafka
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        uvDS.print(">>>>>>>>>>>>>>");
        uvDS.map(json -> json.toJSONString())
                        .addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));

        //TODO 7.启动任务
        env.execute("DwdTrafficUniqueVisitorDetail");
    }
}
