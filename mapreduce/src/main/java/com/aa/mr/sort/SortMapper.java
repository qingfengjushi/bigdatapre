package com.aa.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author AA
 * @Date 2021/11/16 21:14
 * @Project dp11
 * @Package com.aa.mr.sort
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN： 偏移量的  类型
 *     VALUEIN : 一行文本的  类型
 *     KEYOUT：MySortBean
 *     VALUEOUT：NullWritable
 */
public class SortMapper extends Mapper<LongWritable, Text, MySortBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、切分数据
        String[] split = value.toString().split(" ");

        //2、给对应的数据封装到 MySortBean 的实例化对象中。
        MySortBean mySortBean = new MySortBean();
        mySortBean.setWord(split[0]);
        mySortBean.setNum(Integer.parseInt(split[1]));
        System.out.println("----------------------------Mapper--------------------------");
        //3、写出
        context.write(mySortBean,NullWritable.get());
    }
}
