package com.aa.mr.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author AA
 * @Date 2021/11/16 20:22
 * @Project dp11
 * @Package com.aa.mr.partitioner
 * Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 */
public class WordReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //1、定义变量
        long count = 0;

        //2、累加
        for (LongWritable value : values) {
            count += value.get();
        }

        //3、写出去
        context.write(key,new LongWritable(count));
    }
}
