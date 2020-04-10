/*
* Given a text input file, globally and lexicographically orders the words producing two output files.
* */


package sorting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WordSorting {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Input and output required");
            System.exit(2);
        }
        Configuration configuration = new Configuration();
        Path inputPath = new Path(args[0]);
        Path intermediateOutput = new Path(args[1] + "_staging");
        Path partitionFile = new Path(args[1] + "_partitions.lst");
        Path outputDir = new Path(args[1]);
        int code;
        try {
            Job firstJob = Job.getInstance(configuration, "Pre-processing");
            firstJob.setJarByClass(WordSorting.class);
            // Pre-processing stage
            firstJob.setMapperClass(PreProcessingMapper.class);
            firstJob.setNumReduceTasks(0);
            firstJob.setOutputKeyClass(Text.class);
            firstJob.setOutputValueClass(NullWritable.class);
            TextInputFormat.setInputPaths(firstJob, inputPath);
            // Each mapper produces a small file. So we use SequenceFileOutputFormat, that is more
            // efficient in working with small files
            firstJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(firstJob, intermediateOutput);

            code  = firstJob.waitForCompletion(true) ? 0 : 1;
            if (code == 0) {
                Job secondJob = Job.getInstance(configuration, "Sorting");
                // Sorting stage
                secondJob.setMapperClass(Mapper.class);
                secondJob.setReducerClass(Reducer.class);
                secondJob.setOutputKeyClass(Text.class);
                secondJob.setOutputValueClass(NullWritable.class);
                secondJob.setNumReduceTasks(2);

                secondJob.setPartitionerClass(TotalOrderPartitioner.class);
                TotalOrderPartitioner.setPartitionFile(secondJob.getConfiguration(), partitionFile);

                secondJob.setInputFormatClass(SequenceFileInputFormat.class);
                SequenceFileInputFormat.setInputPaths(secondJob, intermediateOutput);
                TextOutputFormat.setOutputPath(secondJob, outputDir);


                // freq = proability to get a key
                // The RandomSampler has the types of the (key,value) pair in the intermediate output
                InputSampler.writePartitionFile(secondJob, new InputSampler.RandomSampler<Text, NullWritable>(0.3, 10));

                code = secondJob.waitForCompletion(true) ? 0 : 2;
            }

            // the second argument of delete() is true when the operation is recursive
            FileSystem.get(new Configuration()).delete(partitionFile, false);
            FileSystem.get(new Configuration()).delete(intermediateOutput, true);


            System.exit(code);


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
