/*
 * Given a text input file, the average length for each first-letter is computed. The MapReduce job is structured
 * as follows:
 * Map: reads the words in a text line and retrieves for each word the (i, l) pair where i is the first letter
 *      and l the length of the word
 * Reduce: computes the average length for a given first letter
 * This implementation uses a Combiner Optimization in order to reduce the amount of intermediate (key,value) pairs
 * */


package numericalSummarization.betteraveragewordlength;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class BetterAverageWordLength {


        public static void main(String[] args) {
            if (args.length != 2) {
                System.err.println("Input and output required");
                System.exit(2);
            }
            Configuration configuration = new Configuration();
            try {
                Job job = Job.getInstance(configuration, "Better Average Word Length");
                job.setJarByClass(BetterAverageWordLength.class);
                job.setMapperClass(BetterAverageWordLengthMapper.class);
                job.setReducerClass(BetterAverageWordLengthReducer.class);
                job.setCombinerClass(BetterAverageWordLengthReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(AvgCountWritable.class);
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
