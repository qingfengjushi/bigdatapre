package com.aa.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author AA
 * @Date 2021/11/16 21:02
 * @Project dp11
 * @Package com.aa.mr.sort
 *
 * a 1
 * a 3
 * b 1
 * a 2
 * c 2
 * c 1
 *
 */
public class MySortBean implements WritableComparable<MySortBean> {
    private String word;
    private int num;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return "MySortBean{" +
                "word='" + word + '\'' +
                ", num=" + num +
                '}';
    }

    /**
     * 实现具体的比较的逻辑
     * @param o
     * @return
     */
    @Override
    public int compareTo(MySortBean o) {
        //1、先对第一列进行排序
        System.out.println("----------------------------CompareTo--------------------------");
        System.out.println(this.word + "---" + o.word);
        System.out.println(this.num + "---" + o.num);
        int i = this.word.compareTo(o.word);

        //2、当第一列相同的时候，再对第二列进行比较
        if (i == 0){
            return this.num - o.num;
        }
        return i;
    }

    /**
     * 序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        System.out.println("----------------------------write--------------------------");
        out.writeUTF(word);
        out.writeInt(num);
    }

    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.word = in.readUTF();
        this.num = in.readInt();
        System.out.println("----------------------------readFields--------------------------");
        System.out.println(this.word + "---" + this.num);
    }
}
