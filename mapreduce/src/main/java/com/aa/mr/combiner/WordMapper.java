package com.aa.mr.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author AA
 * @Date 2021/11/12 21:28
 * @Project dp11
 * @Package com.aa.mr
 * 泛型
 *
 * KEYIN：是指框架读取到的数据集的key的类型，在默认的情况下，读取到的key就是一行的数据相对于文本开头的偏移量。key的类型可不可以是 int、long？LongWritable
 * VALUEIN：是指框架读取到的数据集的value的类型，在默认的情况下，读取到的value就是一行的数据。value的类型可不可以是String？Text
 * KEYOUT：是指用户自定义的业务逻辑方法中返回的数据中的key的类型，由用户根据业务逻辑自己决定的，在我们当前这个wordcount程序中，这个key就是单词，那么这个key的类型可不可以是String类型？Text
 * VALUEOUT：是指用户自定义的业务逻辑方法中返回的数据中的value的类型，由用户根据业务逻辑自己决定的，在我们当前的这个wordcount程序中，这个value就是单词次数，那么这个value可不可以是int、long？LongWritable
 *
 * 但是！
 * Long、Integer、String ... 等等 都是jdk里面的数据类型，在序列化的的时候，效率低。
 * hadoop为了提高效率，自定义了一套序列化的框架 Writable
 * 在hadoop的程序中，如果要进行序列化（写数据、网络传输等等） ，这个时候使用hadoop的实现的序列化的数据类型。
 *
 * Long ——》 LongWritable
 * String ——》 Text
 * Integer --》 IntWritable
 * Null --> NullWritable
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     *
     * @param key 就是偏移量
     * @param value 就是一行文本
     * @param context  上下文，理解成一个消息的载体
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //业务逻辑
        //1、切分单词
        String[] words = value.toString().split(" ");

        //2、计数一次，将单词转化成类似于 <hello,1> 这样的key-value的类型
        for (String word : words) {
            //3、写入到上下文
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
