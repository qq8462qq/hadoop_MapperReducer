package cn.itcast.mapreduce.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReduce extends Reducer<SortBean, NullWritable,SortBean,NullWritable> {
    //reduce方法不作操作(因为以排序好了)
    @Override
    protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
