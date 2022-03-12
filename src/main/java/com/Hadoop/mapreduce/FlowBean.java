package com.Hadoop.mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    //定义上行流量
    private long upFlow;
    //定义下行流量
    private  long downFlow;
    //定义总流量
    private long sumFlow;

    /*序列化框架在反序列化的时候创建对象实例去调用无参构造函数*/
    public FlowBean(){

    }

    public FlowBean(long upFlow,long downFlow,long sumFlow)
    {
        super();
        this.upFlow=upFlow;
        this.downFlow=downFlow;
        this.sumFlow=sumFlow;
    }

    public  FlowBean(long upFlow,long downFlow)
    {
        super();
        this.upFlow=upFlow;
        this.downFlow=downFlow;
        this.sumFlow=upFlow+downFlow;
    }

    public void  set(long upFlow,long downFlow){
        this.upFlow=upFlow;
        this.downFlow=downFlow;
        this.sumFlow=upFlow+downFlow;
    }

    /*
    获取上行流量
     */
    public long getUpFlow() {
        return upFlow;
    }
    /*
    设置上行流量
     */
    public  void  setUpFlow(long upFlow){
        this.upFlow=upFlow;
    }

    /*
    获取下行流量
     */
    public long getDownFlow(){
        return  downFlow;
    }
    /*
    设置下行流量
     */
    public void setDownFlow(long downFlow){
        this.downFlow=downFlow;
    }

    /*
    获取总流量
     */
    public  long getSumFlow(){
        return  sumFlow;
    }
    /*
    设置总流量
     */
    public void setSumFlow(long sumFlow){
        this.sumFlow=sumFlow;
    }

    /*
    序列化方法
     */
    public  void write(DataOutput out) throws IOException{
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }
    /*
    反序列化的方法
    注意：字段的反序列化的顺序跟序列化的顺序必须保持一致
     */
    public void readFields(DataInput in) throws IOException{
        this.upFlow=in.readLong();
        this.downFlow=in.readLong();
        this.sumFlow=in.readLong();
    }

    public  String toString(){
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    /*
    设置进行自定义比较大小的规则
     */
    public  int compareTo(FlowBean o){
        return (int) (o.getSumFlow() - this.getSumFlow());
    }
}
