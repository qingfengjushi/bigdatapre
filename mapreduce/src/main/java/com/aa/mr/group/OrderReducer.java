package com.aa.mr.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author LIAO
 * @create 2020-07-29 20:56
 * Reducer
 *  Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 *      KEYIN：k2 的类型
 *      VALUEIN：v2  的类型
 *      KEYOUT ：k3 一行文本  的类型
 *      VALUEOUT：v3 NullWritable
 *
 */
public class OrderReducer extends Reducer<OrderBean,Text,Text,NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        //获取top N ,下面的代码就是取出来top1。
        for (Text value : values) {
            context.write(value,NullWritable.get());
            i++;
            if (i >= 1){
                break;
            }
        }
    }
}
