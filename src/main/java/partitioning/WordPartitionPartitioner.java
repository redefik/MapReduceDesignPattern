package partitioning;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartitionPartitioner extends Partitioner<Text, Text> {
    public int getPartition(Text key, Text value, int i) {
        if (key.toString().compareTo("m") <= 0) {
            return 0;
        } else {
            return 1;
        }
    }
}
