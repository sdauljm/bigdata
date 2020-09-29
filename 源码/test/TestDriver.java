package lagou.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class TestDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.准备一个配置对象
        Configuration configuration = new Configuration();
        //2.通过配置对象获取一个job对象
        Job job = Job.getInstance(configuration);
        //3.通过job对象设置相关的参数
        //参数包括:驱动类,mapper类,reduce类,输出的key、value的类型、输入文件的路径、结果文件的输出路径
        job.setJarByClass(TestDriver.class);
        job.setMapperClass(TestMapper.class);
        job.setReducerClass(TestReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        //注意:结果文件的输出路径不能事先存在
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        //System.exit(int status)表示退出/终止当前程序,0-表示正常退出,非0-表示异常退出
        System.exit(result ? 0 : 1);
    }
}
