package vn.ghtk.bigdata.entity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class People implements Writable {
    public IntWritable id = new IntWritable();
    public Text firstname = new Text();
    public Text lastname = new Text();
    public Text email = new Text();
    public Text city = new Text();
    public Text profession = new Text();

    public People(){}

    public People(Integer id, String firstname, String lastname, String email, String city, String profession) {
        this.id.set(id);
        this.firstname.set(firstname);
        this.lastname.set(lastname);
        this.email.set(email);
        this.city.set(city);
        this.profession.set(profession);
    }

    public void write(DataOutput out) throws IOException {
        this.id.write(out);
        this.firstname.write(out);
        this.lastname.write(out);
        this.email.write(out);
        this.city.write(out);
        this.profession.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.id.readFields(in);
        this.firstname.readFields(in);
        this.lastname.readFields(in);
        this.email.readFields(in);
        this.city.readFields(in);
        this.profession.readFields(in);
    }

    @Override
    public String toString(){
        return id.get()+","+firstname.toString()+","+lastname.toString()+","+
                email.toString()+","+city.toString()+","+profession.toString();
    }
}
