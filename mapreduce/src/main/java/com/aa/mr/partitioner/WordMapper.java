package com.aa.mr.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author AA
 * @Date 2021/11/16 20:19
 * @Project dp11
 * @Package com.aa.mr.partitioner
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、切分单词
        String[] words = value.toString().split(" ");

        //2、单词统计转换
        for (String word : words) {
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
