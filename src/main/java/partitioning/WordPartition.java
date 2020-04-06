/*
* Given an input file, it creates two shard: one for the words starting with a letter in the first half of the alphabet
* and one with the remaining words.
* */

package partitioning;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class WordPartition {

    public static void main(String[] args) {

        if (args.length != 2) {
            System.err.println("Input and output required");
            System.exit(2);
        }
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration, "Word Partition");
            job.setJarByClass(WordPartition.class);
            job.setMapperClass(WordPartitionMapper.class);
            job.setReducerClass(WordPartitionReducer.class);
            job.setPartitionerClass(WordPartitionPartitioner.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setNumReduceTasks(2);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
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
