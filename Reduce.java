import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
    private Text result = new Text();
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
     // split "cid,sid" pair and write it to output.
        String tmpStr = key.toString();
        String[] parts = tmpStr.split(",");
        String  formattedStr = parts[0] + "\t" + parts[1] + "\t";
        result.set(formattedStr);
        
        context.write(result, new IntWritable(sum));
    }
}
