package com.aa.mr.join.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author AA
 * @Date 2021/9/15 16:25
 * @Project bigdatapre
 * @Package com.aa.join.reducejoin
 * Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 */
public class OPReducer extends Reducer<Text,OrderProductBean,OrderProductBean, NullWritable> {
    /**
     * reduce中处理的就是同一个key的，也就是同一个productid的。
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<OrderProductBean> values, Context context) throws IOException, InterruptedException {
        List<OrderProductBean> orderProductBeans = new ArrayList<OrderProductBean>();
        String productName = "";
        for (OrderProductBean value : values) {
            if ("product".equals(value.getType())) {
                productName = value.getProductname();
            } else if ("order".equals(value.getType())) {
                orderProductBeans.add(value);
            }
        }
        for (OrderProductBean value : orderProductBeans) {
            if ("order".equals(value.getType())) {
                value.setProductname(productName);
                context.write(value, NullWritable.get());
            }
        }
    }
}
