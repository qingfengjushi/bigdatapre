package com.aa.mr.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author AA
 * @Date 2021/11/12 21:54
 * @Project dp11
 * @Package com.aa.mr
 *
 * KEYIN：map阶段输出的key的类型。 Text
 * VALUEIN：map阶段输出的value的类型。 LongWritable
 * KEYOUT：最终的需求结果中的key的类型。 Text
 * VALUEOUT： 最终的需求结果中的value的类型。 LongWritable
 */
public class WordReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    /**
     *
     * @param key 单词
     * @param values 相同单词的次数
     * @param context 上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //1、定义一个变量
        long count = 0;

        //2、迭代累加
        for (LongWritable value : values) {
            count += value.get();
        }

        //3、写入到上下文
        context.write(key,new LongWritable(count));
    }
}
