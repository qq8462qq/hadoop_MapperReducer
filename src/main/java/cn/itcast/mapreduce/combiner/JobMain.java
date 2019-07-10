package cn.itcast.mapreduce.combiner;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//定义主类并继承Configured和实现Tool
public class JobMain extends Configured implements Tool {
    //该方法用于指定一个job任务
    @Override
    public int run(String[] strings) throws Exception {
        //1创建一个job任务对象
        Job job = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());
        //打包到集群上面运行时候，必须要添加以下配置，指定程序的main函数
        job.setJarByClass(JobMain.class);
        //第一步:读取输入文本解析成key,value对(Input)
        job.setInputFormatClass(TextInputFormat.class);
        /*TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/wordcont"));*/
        TextInputFormat.addInputPath(job,new Path("file:///E:\\mapreduce\\input\\combiner_input"));
        //第二步:设置我们的mapper类
        job.setMapperClass(WordCountMap.class);
        //设置我们map阶段完成之后的输出类型
        job.setMapOutputKeyClass(Text.class);//k2类型
        job.setMapOutputValueClass(LongWritable.class);//v2类型
        //第三,(分区)四(排序)省略
        // ,五(规约)
        job.setCombinerClass(wordCountReducer.class);
        // 六(分组)步省略

        //第七步:设置我们的reduce类
        job.setReducerClass(wordCountReducer.class);
        //设置我们reduce阶段完成之后的输出类型
        job.setOutputKeyClass(Text.class);//k3类型
        job.setOutputValueClass(LongWritable.class);//v3类型
        //第八步:设置输出类及输出路径(output)
        job.setOutputFormatClass(TextOutputFormat.class);
        //输出路径
        /*TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/wordcount_out1"));*/
        TextOutputFormat.setOutputPath(job,new Path("file:///E:\\combier_input"));
       //等待任务结束
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }
    /**
         * 程序main函数的入口类
         * @param args
         * @throws Exception
         */

    public static void main(String[] args) throws Exception {
        //启动job任务
        Configuration configuration = new Configuration();
        JobMain tool = new JobMain();
        int run = ToolRunner.run(configuration, tool, args);
        System.exit(run);
    }
}
