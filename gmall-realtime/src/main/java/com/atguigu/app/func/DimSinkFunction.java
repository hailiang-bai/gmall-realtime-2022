package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DruidDSUtil;
import com.atguigu.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private DruidDataSource druidDataSource=null;
    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource  = DruidDSUtil.createDataSource();


    }
    //value:{"database":"gmall","table":"base_trademark","type":"insert","ts":1665738268,"xid":8269,"commit":true":"asd","logo_url":"asda,"data":{"id":12,"tm_named"},"sinktable":"dim_xxx"}
    @Override //来一条数据处理一个
    public void invoke(JSONObject value, Context context) throws Exception {
        //TODO 获取链接
        DruidPooledConnection connection = druidDataSource.getConnection();
        //TODO 写出数据
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");
        //如果写入失败就抛出异常
        PhoenixUtil.upsertValues(connection,sinkTable,data);
        //TODO 归还链接
        connection.close();
    }
}
