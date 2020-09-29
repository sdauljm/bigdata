package lagou.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TestMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
    IntWritable out = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int parseInt = Integer.parseInt(value.toString());
        out.set(parseInt);
        context.write(out,NullWritable.get());
    }
}
