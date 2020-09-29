package lagou.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TestReducer extends Reducer<IntWritable, NullWritable,IntWritable,IntWritable> {
    IntWritable out = new IntWritable(1);
    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(out,key);
            out.set(out.get()+1);
        }
    }
}
