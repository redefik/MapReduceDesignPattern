package structToHier;

import com.google.gson.Gson;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PostCommentReducer extends Reducer<Text, Text, NullWritable, Text> {

    /*
    * The input values include (in any order):
    * - the post with the given identifier (key)
    * - zero, one or more comments related to the post
    * */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] fields;
        Post post = new Post();
        post.setId(key.toString());
        for (Text value : values) {
          fields = value.toString().split(",");
          if (value.charAt(0) == 'P') { // post
              post.setTitle(fields[0].substring(1));
              post.setContent(fields[1]);
          } else { // comment
              Comment comment = new Comment();
              comment.setId(fields[0].substring(1));
              comment.setContent(fields[1]);
              post.addComment(comment);
          }
        }
        // convert to JSON
        Gson gson = new Gson();
        String jsonOutput = gson.toJson(post);
        context.write(NullWritable.get(), new Text(jsonOutput));
    }
}
