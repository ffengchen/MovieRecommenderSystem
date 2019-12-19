import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserRatingMatrixReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		//merge data for one user
		StringBuilder builder = new StringBuilder();
		for(Text value:values){
			builder.append(",").append(value.toString());
		}
		context.write(key, new Text(builder.toString().replaceFirst(",", "")));

	}
}
