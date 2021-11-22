package com.aa.mr.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author AA
 * @Date 2021/11/16 21:20
 * @Project dp11
 * @Package com.aa.mr.sort
 * Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 */
public class SortReducer extends Reducer<MySortBean, NullWritable,MySortBean,NullWritable> {
    @Override
    protected void reduce(MySortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //将map阶段的数据进行汇总输出即可
        System.out.println("----------------------------Reducer--------------------------");
        context.write(key,NullWritable.get());
    }
}
