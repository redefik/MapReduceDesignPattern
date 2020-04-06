/*
* Custom output type. Note: toString() is implemented to have a human-readable output
* */


package numericalSummarization.betteraveragewordlength;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgCountWritable implements Writable {

    private float avg;
    private float count;

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(avg);
        dataOutput.writeFloat(count);
    }

    public void readFields(DataInput dataInput) throws IOException {
        avg = dataInput.readFloat();
        count = dataInput.readFloat();
    }

    public void setAvg(float avg) {
        this.avg = avg;
    }

    public void setCount(float count) {
        this.count = count;
    }

    public float getAvg() {
        return avg;
    }

    public float getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "" + avg;
    }
}
