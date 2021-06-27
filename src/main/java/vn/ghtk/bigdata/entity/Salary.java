package vn.ghtk.bigdata.entity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class Salary implements Writable, Serializable {
    public Text profession = new Text();
    public IntWritable salary = new IntWritable();

    public Salary(){}

    public Salary(String profession, Integer salary) {
        this.profession.set(profession);
        this.salary.set(salary);
    }
    public void write(DataOutput out) throws IOException {
        this.profession.write(out);
        this.salary.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.profession.readFields(in);
        this.salary.readFields(in);
    }
}
