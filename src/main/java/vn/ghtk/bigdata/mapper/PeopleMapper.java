package vn.ghtk.bigdata.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import vn.ghtk.bigdata.entity.People;
import vn.ghtk.bigdata.entity.JoinGenericWritable;

import java.io.IOException;

public class PeopleMapper extends Mapper<LongWritable, Text, Text, JoinGenericWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.get() == 0) return;
        String record = value.toString();
        String[] parts = record.split(",");
        Text prof = new Text(parts[5]);
        People people = new People(Integer.parseInt(parts[0]),parts[1],
                parts[2],parts[3], parts[4], parts[5]);
        JoinGenericWritable joinGenericWritable = new JoinGenericWritable(people);
        context.write(prof, joinGenericWritable);
    }
}
