package cooccurrence;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CoReducer extends Reducer<Text, Text, NullWritable, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		float sumUserRatings = 0;
		int numRatings = 0;
		Vector<String[]> valueCopy = new Vector<String[]>();
		for (Text t : values) {
			String line = t.toString();
			String[] data = line.split(",");
			float userRating = Float.parseFloat(data[2]);
			sumUserRatings += userRating;
			numRatings++;
			valueCopy.add(data);
		}
		float userAverage = sumUserRatings/numRatings;
		float userBias = (float) (userAverage - 3.4811876); // 3.4811876 is the training set average

		int quarterOfItems = 17742/15;
		int lowerBound = (int) (14.2*quarterOfItems);
		int upperBound = (int) (15*quarterOfItems);

		for (int i=0; i<valueCopy.size(); i++) {
			String[] firstItem = valueCopy.get(i);
			int movieId = Integer.parseInt(firstItem[0]);
			if (movieId > lowerBound && movieId < upperBound) { // Limits write to a quarter
				
				for (int j=0; j<valueCopy.size(); j++) {
					if (i == j) {
						continue;
					}
					String[] secondItem = valueCopy.get(j);
					String statPair = firstItem[0] + "," + secondItem[0] + "," +
							firstItem[2] + "," + firstItem[2] + "," +
							firstItem[3] + "," + secondItem[3] + "," +
							firstItem[4] + "," + secondItem[4] + "," +
							userBias;

					context.write(NullWritable.get(), new Text(statPair));
				}
			}
		}
	}
}