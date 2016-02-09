package recommendation;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecommendationReducer extends Reducer<Text, Text, NullWritable, Text> {

	private static double sumOfErrors = 0;
	private static double sumOfSquaredErrors = 0;
	private static int numRated = 0;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		double[] guess = new double[0];
		HashMap<String, Double> ratings = new HashMap<String, Double>();
		for (Text t : values) {
			String tString = t.toString();
			String[] data = tString.split(",");
			if (data.length == 3) {
				double rating = Double.parseDouble(data[2]);
				ratings.put(data[1], rating);
			} else {
				double rating = Double.parseDouble(data[1]);
				guess = new double[] {rating};
			}
		}

		if (ratings.size() > 0) {
			if (guess.length == 0) {
				guess = new double[] {3.4811876};
				return;
			}
			Set<String> ratingKeys = ratings.keySet();
			for (String s : ratingKeys) {
				double d = ratings.get(s);
				guess = new double[]{guess[0]};
				guess[0] = guess[0]>5 ? 5 : guess[0];
				guess[0] = guess[0]<1 ? 1 : guess[0];
				double diff = Math.abs(guess[0] - d);
				numRated++;
				sumOfErrors += diff;
				sumOfSquaredErrors += diff*diff;
			}
		} 

	}

	public void cleanup(Context context) {
		double MAE = sumOfErrors/numRated;
		double RMSD = Math.sqrt(sumOfSquaredErrors/numRated);
		System.out.println("MAE: " + MAE);
		System.out.println("RMSD: " + RMSD);
	}
}