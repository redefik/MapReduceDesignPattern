/*
* Input:
* posts.csv containing for each row: post identifier, title, content
* comments.csv containing for each row: comment identifier, content, post identifier.
* Output:
* denormalizedPost.json
* Each JSON object contains: post identifier, title, content, [list of comments related to the post]
* */

package structToHier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PostCommentImport {

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("<post-file> <comment-file> and output directory required");
            System.exit(2);
        }
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration, "PostComment Import");
            job.setJarByClass(PostCommentImport.class);

            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PostMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CommentMapper.class);
            job.setReducerClass(PostCommentReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.out.println(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
