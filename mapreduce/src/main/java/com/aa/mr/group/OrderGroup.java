package com.aa.mr.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author LIAO
 * @create 2020-07-29 20:48
 * 订单的分组类
 * 实现分组有固定的步骤：
 * 1、继承WritableComparator
 * 2、调用父类的构造器
 * 3、指定分组的规则，重写一个方法
 */
public class OrderGroup extends WritableComparator {

    //1、继承WritableComparator类
    //2、调用父类的构造器
    public OrderGroup(){
        //第一个参数就是分组使用的javabean，第二个参数就是布尔类型，表示是否可以创建这个类的实例
        super(OrderBean.class,true);
    }

    // 3、指定分组的规则，需要重写一个方法

    /**
     * @param a  WritableComparable是接口，Orderbean实现了这个接口
     * @param b WritableComparable是接口，Orderbean实现了这个接口
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //1、对形参a b 做强制类型转换
        OrderBean first = (OrderBean) a;
        OrderBean second = (OrderBean) b;

        //2、指定分组的规则
        return first.getOrderId().compareTo(second.getOrderId());
    }
}
