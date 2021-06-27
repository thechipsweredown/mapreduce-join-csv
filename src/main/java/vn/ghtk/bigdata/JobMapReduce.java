package vn.ghtk.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import vn.ghtk.bigdata.config.ConfigInfo;
import vn.ghtk.bigdata.mapper.PeopleMapper;
import vn.ghtk.bigdata.mapper.SalaryMapper;
import vn.ghtk.bigdata.reducer.JoinReducer;
import vn.ghtk.bigdata.entity.JoinGenericWritable;

import java.io.IOException;
import java.net.URISyntaxException;

public class JobMapReduce {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, ConfigInfo.APP_NAME);
        job.getConfiguration().setStrings(ConfigInfo.SHUFFLE_MEMORY_LIMIT_PERCENT, "0.15");
        job.setJarByClass(JobMapReduce.class);

        Path outputPath = new Path(args[2]);

        job.setReducerClass(JoinReducer.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PeopleMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, SalaryMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinGenericWritable.class);

        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
