package vn.ghtk.bigdata.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import vn.ghtk.bigdata.config.ConfigInfo;
import vn.ghtk.bigdata.entity.People;
import vn.ghtk.bigdata.entity.Salary;
import vn.ghtk.bigdata.entity.JoinGenericWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JoinReducer extends Reducer<Text, JoinGenericWritable, NullWritable, Text> {
    @Override
    public void reduce(Text key, Iterable<JoinGenericWritable> values, Context context)
            throws IOException, InterruptedException {

        List<People> list = new ArrayList<>();

        //find salary in shuffle data
        Salary salary = new Salary("", 0);
        for (JoinGenericWritable j : values) {
            Writable w = j.get();
            if (w instanceof Salary) {
                salary = (Salary) w;
            }
            if (w instanceof People) {
                list.add((People) w);
            }
        }

        //join shuffle data
        for (People people : list) {
            String value = people.toString() + "," + salary.salary.get();
            context.write(NullWritable.get(), new Text(value));

        }
    }

    @Override
    public void run(Reducer<Text, JoinGenericWritable, NullWritable, Text>.Context context) throws IOException, InterruptedException {
        this.setup(context);
        context.write(NullWritable.get(), new Text(ConfigInfo.HEADER));
        try {
            while (context.nextKey()) {
                this.reduce(context.getCurrentKey(), context.getValues(), context);
                Iterator<JoinGenericWritable> iter = context.getValues().iterator();
                if (iter instanceof ReduceContext.ValueIterator) {
                    ((ReduceContext.ValueIterator) iter).resetBackupStore();
                }
            }
        } finally {
            this.cleanup(context);
        }

    }
}
