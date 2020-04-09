package wordGrep;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordGrepMapper extends Mapper<Object, Text, NullWritable, Text> {

    private String regexp;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        regexp = context.getConfiguration().get("regexp");
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().matches(regexp)) {
            context.write(NullWritable.get(), value);
        }
    }
}
