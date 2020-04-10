package structToHier;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommentMapper extends Mapper<Object, Text, Text, Text> {

    // The input is a single CSV line
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String postId = line.substring(line.lastIndexOf(",") + 1);
        String commentAttributes = "C" + line.substring(0, line.lastIndexOf(","));
        // Emit (post identifier, comment attributes)
        context.write(new Text(postId), new Text(commentAttributes));
    }
}
