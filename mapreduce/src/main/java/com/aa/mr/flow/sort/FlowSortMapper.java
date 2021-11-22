package com.aa.mr.flow.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、拆分文本的数据
        String[] split = value.toString().split("\t");
        //2、封装到 FlowBean 的实例对象中
        FlowBean flowBean = new FlowBean();
        flowBean.setPhoneNum(split[0]);
        flowBean.setUpFlow(Integer.parseInt(split[1]));
        flowBean.setDownFlow(Integer.parseInt(split[2]));
        flowBean.setUpCountFlow(Integer.parseInt(split[3]));
        flowBean.setDownCountFlow(Integer.parseInt(split[4]));

        //3、写出
        context.write(flowBean, value);
    }
}
