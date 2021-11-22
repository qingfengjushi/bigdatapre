package com.aa.mr.join.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author AA
 * @Date 2021/9/15 12:56
 * @Project bigdatapre
 * @Package com.aa.join.reducejoin
 */
public class OrderProductBean implements Writable {
    /**join_order表
     * 订单id	商品id	 数量
     * orderid	productid	amount
     * 00001	0001	2
     * 00002	0002	4
     *
     * join_product 表
     * 商品id		商品名字
     * productid	productname
     * 0001	美的冰箱
     * 0002	海尔冰箱
     */
    private String orderid; //订单id
    private String productid;  //商品id
    private Integer amount; //数量
    private String productname; //商品名字
    private String type; //用来判断是来自于orders表还是product表

    public String getOrderid() {
        return orderid;
    }

    public void setOrderid(String orderid) {
        this.orderid = orderid;
    }

    public String getProductid() {
        return productid;
    }

    public void setProductid(String productid) {
        this.productid = productid;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public String getProductname() {
        return productname;
    }

    public void setProductname(String productname) {
        this.productname = productname;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * 在这里处理成自己想要输出的样子就可以了。
     * @return
     */
    @Override
    public String toString() {
        return orderid +
                "\t" + productid +
                "\t" + amount +
                "\t" + productname;
    }

    /**
     * 序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderid);
        out.writeUTF(productid);
        out.writeInt(amount);
        out.writeUTF(productname);
        out.writeUTF(type);
    }

    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderid = in.readUTF();
        this.productid = in.readUTF();
        this.amount = in.readInt();
        this.productname = in.readUTF();
        this.type = in.readUTF();
    }

}
