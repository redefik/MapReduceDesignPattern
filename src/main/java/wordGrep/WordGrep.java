package wordGrep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordGrep {

    public static void main(String[] args) {

        if (args.length != 3) {
            System.err.println("Input, output and regexp required");
            System.exit(2);
        }
        Configuration configuration = new Configuration();
        configuration.set("regexp", args[2]);
        try {
            Job job = Job.getInstance(configuration, "Word Grep");
            job.setJarByClass(WordGrep.class);
            job.setMapperClass(WordGrepMapper.class);
            job.setNumReduceTasks(0);

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
