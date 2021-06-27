package vn.ghtk.bigdata.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import vn.ghtk.bigdata.entity.Salary;
import vn.ghtk.bigdata.entity.JoinGenericWritable;

import java.io.IOException;

public class SalaryMapper extends Mapper<LongWritable, Text, Text, JoinGenericWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.get() == 0) return;
        String record = value.toString();
        String[] parts = record.split(",");
        Salary salary = new Salary(parts[0], Integer.parseInt(parts[1]));
        JoinGenericWritable joinGenericWritable = new JoinGenericWritable(salary);
        context.write(new Text(parts[0]), joinGenericWritable);
    }
}
