package com.aa.mr.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author AA
 * @Date 2021/11/16 21:48
 * @Project dp11
 * @Package com.aa.mr.combiner
 * Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 *     KEYIN: map阶段传递过来的 key的类型
 *     VALUEIN: map阶段传递过来的 value的类型
 *     KEYOUT:局部合并的之后的key的类型。 在本案例当中，其实就是单词的类型
 *     VALUEOUT: 局部合并之后的value的类型。在本案例中，其实就是单词的次数
 */
public class MyCombiner extends Reducer<Text, LongWritable,Text,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //1、定义一个变量
        long count = 0;

        //2、进行累加
        for (LongWritable value : values) {
            count += value.get();
        }

        //3、写出
        context.write(key,new LongWritable(count));
    }
}
