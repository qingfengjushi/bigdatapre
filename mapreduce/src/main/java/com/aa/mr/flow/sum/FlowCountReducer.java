package com.aa.mr.flow.sum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author AA
 * @Date 2021/11/16 22:21
 * @Project dp11
 * @Package com.aa.mr.flow.sum
 * Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 *
 *  | 上行数据包 | upFlow        | int    |
 *  | 下行数据包 | downFlow      | int    |
 *  | 上行流量   | upCountFlow   | int    |
 *  | 下行流量   | downCountFlow | int    |
 */
public class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //1、遍历values，将对应的你想要统计的数据进行累加
        Integer upFlow = 0;
        Integer downFlow = 0;
        Integer upCountFlow = 0;
        Integer downCountFlow = 0;
        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }

        //2、创建一个结果的实例对象，存放累加之后的结果
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);

        //3、写出
        context.write(key,flowBean);
    }
}
