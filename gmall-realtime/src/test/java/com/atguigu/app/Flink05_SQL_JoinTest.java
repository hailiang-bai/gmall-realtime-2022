package com.atguigu.app;

import com.atguigu.bean.WaterSensor;
import com.atguigu.bean.WaterSensor2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class Flink05_SQL_JoinTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置流中状态的保存时间
        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //1001,23.6,1324
        SingleOutputStreamOperator<WaterSensor> waterSensorDS1 = env.socketTextStream("hadoop102", 8888)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] split = element.split(",");
                        return new Long(split[2]) * 1000L;
                    }
                })).map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Double.parseDouble(split[1]),
                            Long.parseLong(split[2]));
                });


        SingleOutputStreamOperator<WaterSensor2> waterSensorDS2 = env.socketTextStream("hadoop102", 9999)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] split = element.split(",");
                        return new Long(split[2]) * 1000L;
                    }
                })).map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor2(split[0],
                            split[1],
                            Long.parseLong(split[2]));
                });

        //将流转化为动态表
        //只有创建了临时视图动态表，后续才能使用fLinkSQL 的SQLQuery等查询
        tableEnv.createTemporaryView("t1",waterSensorDS1);
        tableEnv.createTemporaryView("t2", waterSensorDS2);

        //FlinkSQL JOIN


        //Inner join 左表 OnCreateAndWrite 右表 OnCreateAndWrite 状态保存问题，
//        tableEnv.sqlQuery("select t1.id,t1.vc,t2.id,t2.name from t1 join t2 on t1.id = t2.id")
//                .execute()
//                .print();
        // left join 左表 OnReadeAndWrite 右表 OnCreateAndWrite 状态保存问题，如果右表一直有数据，那左表被读更新过期时间
//        tableEnv.sqlQuery("select t1.id,t1.vc,t2.id,t2.name from t1 left join t2 on t1.id = t2.id")
//                .execute()
//                .print();

//        // right join 左表 OnCreateAndWrite 右表 OnReadeAndWrite 状态保存问题，如果右表一直有数据，那左表被读更新过期时间
//        tableEnv.sqlQuery("select t1.id,t1.vc,t2.id,t2.name from t1 right join t2 on t1.id = t2.id")
//                .execute()
//                .print();

        // full join 左表 OnReadeAndWrite 右表 OnReadeAndWrite 状态保存问题，如果右表一直有数据，那左表被读更新过期时间
        tableEnv.sqlQuery("select t1.id,t1.vc,t2.id,t2.name from t1 full join t2 on t1.id = t2.id")
                .execute()
                .print();
    }


}
