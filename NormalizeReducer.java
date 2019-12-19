import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		//key = movieA, value=<movieB:relation, movieC:relation...>
		//normalize each unit of co-occurrence matrix
		// outputKey : movieB
		// outputValue : movieA=(normalized value)
		
		int sum = 0;
		String movieA = key.toString().trim();

		Map<String, Integer> map = new HashMap<>();
		for(Text value: values){
			String[] movie_rating = value.toString().trim().split(":");

			int relation = Integer.parseInt(movie_rating[1]);
			sum+=relation;
			map.put(movie_rating[0],relation);
		}
		StringBuilder builder = new StringBuilder();
		for(Map.Entry<String,Integer> entry:map.entrySet()){
			builder.setLength(0);
			String movieB = entry.getKey();
			int relation = entry.getValue();

			builder.append(movieA).append("=").append((double)relation/sum);
			context.write(new Text(movieB), new Text(builder.toString()));
		}
	}
}
