package similarity;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarReducer extends Reducer<Text, Text, NullWritable, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int numValues = 0;

		float firstMean = 0;
		float secondMean = 0;
		float NdotProd = 0;
		double Nrating1SquaredSum = 0;
		double Nrating2SquaredSum = 0;
		String[] stats = new String[0];
		for (Text t : values) {
			String line = t.toString();
			stats = line.split(",");

			float bias = Float.parseFloat(stats[8]);
			
			float rating1 = Float.parseFloat(stats[2]) - bias;
			float rating2 = Float.parseFloat(stats[3]) - bias;
			

			if (firstMean == 0) {
				float total1 = Float.parseFloat(stats[4]);
				float total2 = Float.parseFloat(stats[5]);
				float sum1 = Float.parseFloat(stats[6]);
				float sum2 = Float.parseFloat(stats[7]);
				firstMean = sum1/total1;
				secondMean = sum2/total2;
			}

			NdotProd += (rating1 - firstMean)*(rating2 - secondMean);
			Nrating1SquaredSum += (rating1 - firstMean)*(rating1 - firstMean);
			Nrating2SquaredSum += (rating2 - secondMean)*(rating2 - secondMean);

			numValues++;
		}
		
		float movieBias = (float) (firstMean - 3.4811876);

		if (numValues > 10) {
			double pearson = (NdotProd/(Math.sqrt(Nrating1SquaredSum)*Math.sqrt(Nrating2SquaredSum)));
			pearson = Math.abs(pearson);
			context.write(NullWritable.get(), new Text(stats[0] + ',' + stats[1] + "," 
					+ pearson + ',' + secondMean + ',' + movieBias + ',' + firstMean));
		}
	}
}