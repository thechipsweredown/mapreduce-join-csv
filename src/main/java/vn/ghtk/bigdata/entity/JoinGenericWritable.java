package vn.ghtk.bigdata.entity;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;
import vn.ghtk.bigdata.entity.People;
import vn.ghtk.bigdata.entity.Salary;

public class JoinGenericWritable extends GenericWritable {

    private static Class<? extends Writable>[] CLASSES = null;

    static {
        CLASSES = (Class<? extends Writable>[]) new Class[] {
                People.class,
                Salary.class
        };
    }

    public JoinGenericWritable() {}

    public JoinGenericWritable(Writable instance) {
        set(instance);
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }
}