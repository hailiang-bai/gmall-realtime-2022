package com.atguigu.app.func;

import com.atguigu.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String str)  {
        //        for (String s : str.split(" ")) {
//            collect(Row.of(s, s.length()));
//        }
        List<String> list = null;
        try {
            list = KeyWordUtil.splitKeyWord(str);
            for (String word : list) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(str));
        }
    }
}
