import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapReduceIMDB 
{

    private static final String FAVORITE_ACTOR = "Willem Dafoe";
	private static final int SPLIT = 1024 * 1024; // 1 MB

    // Title Map ---------------------------------------------------------------------------
    public static class TitleMapper extends Mapper<LongWritable, Text, Text, Text> 
	{

        private Text k = new Text();
        private Text v = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
            String line = value.toString();
            if (line.startsWith("tconst") || line.length() == 0) 
				return;

            String[] tokens = line.split("\t", -1);

            String id = tokens[0].trim();
            String type = tokens[1].trim();
            String year = tokens[5].trim();

			if (year.equals("\\N") || year.length() == 0) 
				return;

			if (!type.equals("movie"))
				return;

            k.set(id);
            v.set(year);
            context.write(k, v);
        }
    }

    // Actor Map ----------------------------------------------------------------------------
    public static class ActorMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
        private Text k = new Text();
        private Text v = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
            String line = value.toString();
            String[] tokens = line.split(",", 3);

            String titleId = tokens[0].trim();
            String actorName = tokens[2].trim();

            if (titleId.length() == 0 || actorName.length() == 0) 
				return;

			if (!actorName.equals(FAVORITE_ACTOR))
				return;

            k.set(titleId);
            v.set(actorName);
            context.write(k, v);
        }
    }

    // Reducer to Join Titles and Actors -------------------------------------------------------
    public static class JoinReducer extends Reducer<Text, Text, Text, IntWritable> 
	{
		private final static IntWritable ONE = new IntWritable(1);
		private Text k = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			String year = null;
			boolean hasActor = false;

			for (Text t : values) 
			{
				String s = t.toString();
				// if no year yet, must be from titles
				if (year == null) 
				{
					year = s; 
				} 
				else 
				{
					hasActor = true;
				}
			}

			if (hasActor && year != null) 
			{
				k.set(FAVORITE_ACTOR + "|" + year);
				context.write(k, ONE);
			}
		}
	}


    // Job2 -------------------------------------------------------------------------------------------------
    // Mapper for Job,  passes through the key and emits 1
	public static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
	{ 
		private final static IntWritable ONE = new IntWritable(1);
		private Text k = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String line = value.toString().trim();
			if (line.isEmpty()) 
				return;

			String[] tokens = line.split("\t");
			if (tokens.length < 2) 
				return;

			k.set(tokens[0]);
			context.write(k, ONE);
		}
	}


    // Reducer for Job2, sums amount of movies per year by Willem Dafoe -----------------------------------------------------------------------------
    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
            int sum = 0;
            for (IntWritable v : values)
				sum += v.get();

            result.set(sum);
            context.write(key, result);
        }
    }

    // Main with configs -----------------------------------------------------------------------------------------
    public static void main(String[] args) throws Exception 
	{
		// MapReduceIMDB <titlesInput> <actorsInput> <intermediateOutput> <finalOutput> <job1Reducers> <job2Reducers> <mode: partA|partB>

		String titlesInput = args[0];
		String actorsInput = args[1];
		String job1Output = args[2];
		String output = args[3];
		int job1Reducers = Integer.parseInt(args[4]);
		int job2Reducers = Integer.parseInt(args[5]);
		String mode = args[6];

		boolean isPartB = false;
		if(mode.equals("partB")) {
			isPartB = true;
		}

		// Job 1 configuration --------------------------------------------------------------
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "IMDB Join Movies and Actors and Filter");

		job1.setJarByClass(MapReduceIMDB.class);

		job1.setReducerClass(JoinReducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		MultipleInputs.addInputPath(job1, new Path(titlesInput), TextInputFormat.class, TitleMapper.class);
		MultipleInputs.addInputPath(job1, new Path(actorsInput), TextInputFormat.class, ActorMapper.class);
		FileOutputFormat.setOutputPath(job1, new Path(job1Output));

		job1.setNumReduceTasks(job1Reducers);

		// multiple mapper tasks per input if partb
		if (isPartB) {
			FileInputFormat.setMaxInputSplitSize(job1, SPLIT);
		}

		job1.waitForCompletion(true);

		// Job 2 configuration --------------------------------------------------------------
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "IMDB_Count_Actor");
		job2.setJarByClass(MapReduceIMDB.class);

		job2.setMapperClass(CountMapper.class);
		job2.setReducerClass(SumReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(job1Output));
		FileOutputFormat.setOutputPath(job2, new Path(output));

		job2.setNumReduceTasks(job2Reducers);

		if (isPartB) {
			FileInputFormat.setMaxInputSplitSize(job2, SPLIT); // 1 MB splits for multiple mappers
		}

		job2.waitForCompletion(true);
	}
}
