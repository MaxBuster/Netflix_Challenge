package preproccess;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Vector<String> valueCopy = new Vector<String>();
		int numRatings = 0;
		float sumRatings = 0;
		for (Text t : values) {
			String line = t.toString();
			String[] data = line.split(",");
			float rating = Float.parseFloat(data[2]);
			numRatings++;
			sumRatings += rating;
			valueCopy.add(line);
		}
		for (String t : valueCopy) {
			context.write(NullWritable.get(), new Text(t + ',' + numRatings + ',' + sumRatings));
		}
	}
}