package structToHier;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class PostMapper extends Mapper<Object, Text, Text, Text> {

    // The input is a single CSV line
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String postId = line.substring(0, line.indexOf(","));
        String postAttributes = "P" + line.substring(line.indexOf(",") + 1);
        // Emit (post identifier, post attributes)
        context.write(new Text(postId), new Text(postAttributes));
    }
}
