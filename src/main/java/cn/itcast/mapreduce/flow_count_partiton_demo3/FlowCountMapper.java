package cn.itcast.mapreduce.flow_count_partiton_demo3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text,Text, FlowBean> {
    /*
    * 将k1,和v1转成k2.v2
    *
    * */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1拆分行文本数据,得到手机号----k2
        String[] split = value.toString().split("\t");
        String phoneNum=split[1];
        //2创建FlowBean对象,从行文本数据得到流量的四个字段

        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[6]));
        flowBean.setDownFlow(Integer.parseInt(split[7]));
        flowBean.setUpCountFlow(Integer.parseInt(split[8]));
        flowBean.setDownCountFlow(Integer.parseInt(split[9]));
        //3将k2v2写入上下文中
        context.write(new Text(phoneNum),flowBean);
    }
}
