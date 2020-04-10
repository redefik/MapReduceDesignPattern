/*
* Givent an input file, retrieves the unique words in the file.
* */

package distinct;

import numericalSummarization.averagewordlength.AverageWordLength;
import numericalSummarization.averagewordlength.AverageWordLengthMapper;
import numericalSummarization.averagewordlength.AverageWordLengthReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import wordGrep.WordGrep;

import java.io.IOException;

public class WordDistinct {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Input and output required");
            System.exit(2);
        }
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration, "Word distinct");
            job.setJarByClass(WordDistinct.class);
            job.setMapperClass(WordDistinctMapper.class);
            job.setReducerClass(WordDistinctReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
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
