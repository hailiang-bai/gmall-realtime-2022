package com.atguigu.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.taskexecutor.JobTable;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //{"before":null,"after":{"source_table":"11","sink_table":"ds","sink_columns":"wsqw","sink_pk":"qw","sink_extend":"qw"},
    // "source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1665625070795,"snapshot":"false","db":"gmall-211126-config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1665625070799,"transaction":null}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        //TODO 1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //TODO 2.校验并建表
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());

        //TODO 3.写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(), tableProcess);

    }

    /**
     * 校验并建表
     * @param sinkTable    phoenix表名
     * @param sinkColumns  phoenix表字段
     * @param sinkPk       phoenix表主键
     * @param sinkExtend   phoenix表扩展字段
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement=null;
        try {
            //处理特殊字段（null字段）
            if(sinkPk==null || "".equals(sinkPk)){
                sinkPk="id";
            }

            if(sinkExtend==null){
                sinkExtend="";
            }
            //拼接SQL create table if not exits db.tn(id varchar primary key,bb varchar) xxx
            StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                //判断是否为主键
                if (sinkPk.equals(column)) {
                    createTableSql.append(column).append(" varchar primary key");
                }else{
                    createTableSql.append(column).append(" varchar");
                }
                //判断是否是最后一个字段
                if(i<columns.length-1){
                    createTableSql.append(",");
                }
            }

            createTableSql.append(")").append(sinkExtend);
            //编译SQL
            System.out.println("建表语句："+createTableSql);
             preparedStatement = connection.prepareStatement(createTableSql.toString());
            //执行SQL，建表
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException("建表失败："+sinkTable);
        } finally {
            //释放资源
            if(preparedStatement != null ){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    // value得到的值是46张表，包含事实表和维度表，json字符串中 table的value值是表名，data是表的内容
    //{"database":"gmall","table":"base_trademark","type":"insert","ts":1665738268,"xid":8269,"commit":true":"asd","logo_url":"asda,"data":{"id":12,"tm_named"}}
    @Override //读状态
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //TODO 1.获得广播配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String table = value.getString("table");
//        System.out.println(table+"--------------------------------");
        //上面广播表写状态里面已经把k-v写进去了，对应的是原表+广播表，53行左右，broadcastState.put(tableProcess.getSourceTable(), tableProcess);
        TableProcess tableProcess = broadcastState.get(table); //事实表为null，因为广播流中只有维度表
//        System.out.println("+++++++++++++++++++"+broadcastState);
        /*base_trademark=TableProcess(sourceTable=base_trademark,
                sinkTable=dim_base_trademark,
                sinkColumns=id,
                tm_name,
                sinkPk=id,
                sinkExtend=null)*/
        if (tableProcess != null) {
            //TODO 2.过滤字段
            filterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());

            //TODO 3.补充sinktable并写出到流中
            value.put("sinkTable", tableProcess.getSinkTable());
            out.collect(value);
        } else {
            System.out.println("找不到对应的key：" + table);
        }

    }

    /**
     * 过滤字段
     * @param data     {“id” :13 ,"tm_name" : "atguigu" ,"logo_url": "xxxxxx"}
     * @param sinkColumns “id,tm_mame"
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        //切分sinkColumns
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);
        //JSONObject 当做map
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            if(!columnList.contains(next.getKey())){
                iterator.remove();
            }
        }

    }
}
