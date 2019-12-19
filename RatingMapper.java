import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RatingMapper extends Mapper<Object, Text, Text, Text> {
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		//input: user,movie,rating
		//pass data to reducer
		// output key: movieID
		// output value: user:rating
		String[] user_movieRatings = value.toString().trim().split(",");
		StringBuilder builder = new StringBuilder();
		builder.append(user_movieRatings[0]).append(":").append(user_movieRatings[2]);
		context.write(new Text(user_movieRatings[1]),new Text(builder.toString()));
		
	}
}
