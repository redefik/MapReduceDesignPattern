package numericalSummarization.betteraveragewordlength;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BetterAverageWordLengthReducer extends Reducer<Text,AvgCountWritable, Text, AvgCountWritable> {
    @Override
    protected void reduce(Text key, Iterable<AvgCountWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        float count = 0;
        for (AvgCountWritable value : values) {
            sum =  sum + (value.getAvg()*value.getCount());
            count += value.getCount();
        }
        AvgCountWritable avgCountWritable = new AvgCountWritable();
        avgCountWritable.setAvg(sum / count);
        avgCountWritable.setCount(count);
        context.write(key, avgCountWritable);
    }
}
