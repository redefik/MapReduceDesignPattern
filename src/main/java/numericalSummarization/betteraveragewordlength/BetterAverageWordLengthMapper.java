package numericalSummarization.betteraveragewordlength;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class BetterAverageWordLengthMapper extends Mapper<Object, Text, Text, AvgCountWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().toLowerCase();
            int wordLength = word.length();
            AvgCountWritable avgCountWritable = new AvgCountWritable();
            avgCountWritable.setAvg(wordLength);
            avgCountWritable.setCount(1);
            context.write(new Text(word.substring(0,1)), avgCountWritable);
        }
    }
}
