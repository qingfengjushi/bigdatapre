package com.aa.mr.group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author LIAO
 * @create 2020-07-29 20:25
 * 订单实体类
 */
public class OrderBean implements WritableComparable<OrderBean>{

    private String orderId; //订单编号
    private Double price; //订单中某个商品的价格

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "orderId='" + orderId + '\'' +
                ", price=" + price +
                '}';
    }

    /**
     * @param o 实体参数
     * @return
     * 指定排序的规则
     */
    @Override
    public int compareTo(OrderBean o) {
        //1、先比较订单的id，如果id一样，则将订单的商品按金额排序（降序）
        int i = this.orderId.compareTo(o.orderId);
        if (i == 0){
            //因为是降序，所以有-1
            i = this.price.compareTo(o.price) * -1;
            //i = o.price.compareTo(this.price);
        }
        return i;
    }

    /**
     * 实现对象的序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    /**
     * 实现对象的反序列化
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.price = in.readDouble();
    }
}
