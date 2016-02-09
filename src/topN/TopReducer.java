package topN;
import java.io.IOException;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.commons.math.stat.regression.SimpleRegression;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopReducer extends Reducer<Text, Text, NullWritable, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		TreeMap<Double, String> topN = new TreeMap<Double, String>();
		for (Text t : values) {
			String line = t.toString();
			String[] stats = line.split(",");
			double similarity = Double.parseDouble(stats[2]);
			topN.put(similarity, line);
		}

		float movieBias = 0;
		double sumSimAndRate = 0;
		double sumSim = 0;
		for (int i=0 ; i<topN.size(); i++) {
			String line = topN.get(topN.firstKey());
			topN.remove(topN.firstKey());
			String[] stats = line.split(",");
			double similarity = Double.parseDouble(stats[2]);
			sumSim += similarity;
			double rating = (Double.parseDouble(stats[3])-3.4811876);
			sumSimAndRate += rating*similarity;
			movieBias = Float.parseFloat(stats[4]);
		}
		double weightedAverage = sumSimAndRate/sumSim;

		double averageWithMovieBias = 3.4811876 + (weightedAverage/100) + movieBias;

		context.write(NullWritable.get(), new Text(key.toString() + ',' + averageWithMovieBias));
	}
}