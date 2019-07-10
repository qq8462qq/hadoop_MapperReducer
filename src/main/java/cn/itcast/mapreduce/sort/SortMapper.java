package cn.itcast.mapreduce.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text,SortBean, NullWritable> {
    //map方法把k1,v1转成k2,v2
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1将行文本拆分,将数据封装到SortBean对象
        SortBean sortBean = new SortBean();
        String[] split = value.toString().split("\t");
        sortBean.setWord(split[0]);
        sortBean.setNum(Integer.parseInt(split[1]));
        //2写入上下文对象
        context.write(sortBean,NullWritable.get());
    }
}
