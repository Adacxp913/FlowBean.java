package com.Hadoop.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class FlowSum {

    /*在kv（key value）中是可以传输我们的自定义对象的，但是必须要实现Hadoop的序列化机制，也就是implement Writable*/
    public static class FlowSumMapper extends Mapper<LongWritable, Text,Text,FlowBean>{
        Text k=new Text();
        FlowBean v=new FlowBean();

        @Override
        protected void map(LongWritable key,Text value,Context context)
                throws IOException,InterruptedException{
            /*将读取到的每一行数据进行字段的切分*/
            String line=value.toString();
            String[] fields= StringUtils.split(line,"\t");

            /*抽取我们也无所需要的字段*/
            String phoneNum=fields[1];
            long upFlow=Long.parseLong(fields[fields.length-3]);
            long downFlow=Long.parseLong(fields[fields.length-2]);

            k.set(phoneNum);
            v.set(upFlow,downFlow);

            context.write(k,v);
        }
    }

    public  static  class FlowSumReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
        FlowBean v=new FlowBean();

        /*Reduce方法接收到的key就是某一组《a手机号，bean》 《a手机号，bean》  《b手机号，bean》 《b手机号，bean》当中的第一个手机号*/
        /*reduce方法接收到的value就是这一组kv对中的bean的一个迭代器*/
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context)
                throws IOException, InterruptedException {

            long upFlowCount = 0;
            long downFlowCount = 0;

            for (FlowBean bean : values) {
                upFlowCount += bean.getUpFlow();
                downFlowCount += bean.getDownFlow();
            }
            v.set(upFlowCount, downFlowCount);
            context.write(key, v);
        }
    }

    /*
    主函数
     */
    public static void main(String[] args) throws Exception{

        /*自动快速地使用缺省Log4j环境*/
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowSum.class);

        /*告诉程序，我们的程序所用的mapper类和reducer类是什么*/
        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

        /*告诉框架，我们程序输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        */

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        /*告诉框架，我们程序使用的数据读取组件，结果输出所用的组件是什么*/
        //TextInputFormat是mapreduce程序中内置的一种读取数据组件  准确的说 叫做 读取文本文件的输入组件
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //告诉框架，我们要处理的数据文件在那个路径下
        /*注意：新建 /week2Homework/input        hadoop fs -mkdir -p /chengxiaoping/week2Homework/input*/
        /*把数据放在 /week2Homework/input 目录下  hadoop fs -put 3.txt /chengxiaoping/week2Homework/input*/
        FileInputFormat.setInputPaths(job,new Path("/chengxiaoping/week2Homework/input"));
        //告诉框架，我们的处理结果要输出到什么地方
        FileOutputFormat.setOutputPath(job,new Path("/chengxiaoping/week2Homework/output"));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0:1 ) ;

        /*把打包后的jar上传
        运行   hadoop jar FlowSum-1.0.jar com.Hadoop.mapreduce.FlowSum
        */
    }
}
