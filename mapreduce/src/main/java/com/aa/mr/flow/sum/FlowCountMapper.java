package com.aa.mr.flow.sum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author AA
 * @Date 2021/11/16 22:14
 * @Project dp11
 * @Package com.aa.mr.flow.sum
 *  Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *      KEYIN:偏移量的类型
 *      VALUEIN：一行数据文本的类型
 *      KEYOUT：手机号的类型
 *      VALUEOUT：FlowBean
 *
 *1363157995033 	15920133257	5C-0E-8B-C7-BA-20:CMCC	120.197.40.4	sug.so.360.cn	信息安全	20	        20	        3156	  2936	    200
 * 时间戳			手机号		基站编号				IP				URL				URL类型		上行数据包	下行数据包  上行流量  下行流量  响应
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、拆分文本的数据
        String[] split = value.toString().split("\t");
        String phoneNum = split[1];

        //2、封装到 FlowBean 的实例对象中
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[6]));
        flowBean.setDownFlow(Integer.parseInt(split[7]));
        flowBean.setUpCountFlow(Integer.parseInt(split[8]));
        flowBean.setDownCountFlow(Integer.parseInt(split[9]));

        //3、写出
        context.write(new Text(phoneNum),flowBean);
    }
}
