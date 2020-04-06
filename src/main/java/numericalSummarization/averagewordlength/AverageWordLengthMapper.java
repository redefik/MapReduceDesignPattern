package numericalSummarization.averagewordlength;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class AverageWordLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // TODO
        // suddividere la linea in parole
        // per ogni parola emettere iniziale e lunghezza della parola
        // per la suddivisione utilizziamo lo StringTokenizer di Java
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().toLowerCase();
            int wordLength = word.length();
            context.write(new Text(word.substring(0,1)), new IntWritable(wordLength));
        }
    }
}
