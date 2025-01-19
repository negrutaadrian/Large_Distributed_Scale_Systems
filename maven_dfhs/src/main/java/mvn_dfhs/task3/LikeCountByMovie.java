package mvn_dfhs.task3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * LikeCountByMovie is a two-stage MapReduce program that:
 * 1. Counts the number of users who liked each movie.
 * 2. Groups movies by their like-count.
 *
 * Example Output:
 * 40    Toy Story Alien
 */
public class LikeCountByMovie {

    /**
     * Job 1 Mapper: Extracts MovieName from each line and emits (MovieName, 1).
     * Input Format: userID \t MovieName
     * Output Format: MovieName \t 1
     */
    public static class MovieNameMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text movieName = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Split the line by tab
            String[] parts = value.toString().trim().split("\\t");
            if (parts.length == 2) {
                String moviePart = parts[1].trim();
                // Extract the movie name up to the last parenthesis
                int lastParenIndex = moviePart.lastIndexOf(')');
                if (lastParenIndex != -1) {
                    String _movieName = moviePart.substring(0, lastParenIndex + 1).trim();
                    movieName.set(_movieName);
                    context.write(movieName, one);
                }
            }
            // If the line doesn't have exactly 2 parts, it's malformed and can be skipped
        }
    }

    /**
     * Job 1 Reducer: Sums up the counts for each MovieName.
     * Input Format: MovieName \t Iterable<1>
     * Output Format: MovieName \t TotalLikeCount
     */
    public static class MovieCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable totalCount = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }
            totalCount.set(sum);
            context.write(key, totalCount);
        }
    }

    /**
     * Job 2 Mapper: Inverts the output of Job 1 by emitting (LikeCount, MovieName).
     * Input Format: MovieName \t LikeCount
     * Output Format: LikeCount \t MovieName
     */
    public static class CountMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable likeCount = new IntWritable();
        private Text movieName = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Split the line by tab
            String[] parts = value.toString().trim().split("\\t");
            if (parts.length == 2) {
                String movie = parts[0].trim();
                String countStr = parts[1].trim();
                try {
                    int count = Integer.parseInt(countStr);
                    likeCount.set(count);
                    movieName.set(movie);
                    context.write(likeCount, movieName);
                } catch (NumberFormatException e) {
                    // If count is not a valid integer, skip this record
                }
            }
            // If the line doesn't have exactly 2 parts, it's malformed and can be skipped
        }
    }

    /**
     * Job 2 Reducer: Groups MovieNames by their LikeCount.
     * Input Format: LikeCount \t Iterable<MovieName>
     * Output Format: LikeCount \t MovieName1 MovieName2 ...
     */
    public static class GroupMoviesReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private Text moviesList = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for(Text val : values){
                sb.append(val.toString()).append(" "); // insert spaces between each movie name
            }
            if (sb.length() > 0) {
                sb.setLength(sb.length() - 1); // Remove the trailing space
            }
            moviesList.set(sb.toString());
            context.write(key, moviesList);
        }
    }

    /**
     * Driver method to run both MapReduce jobs sequentially.
     *
     * Usage: LikeCountByMovie <input_path> <intermediate_output_path> <final_output_path>
     */

    public static void main(String[] args) throws Exception {
        if(args.length != 3){
            System.err.println("Usage: LikeCountByMovie <input_path> <intermediate_output_path> <final_output_path>");
            System.exit(-1);
        }

        // Configuration and Job setup for Job 1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Count Users per Movie");
        job1.setJarByClass(LikeCountByMovie.class);
        job1.setMapperClass(MovieNameMapper.class);
        job1.setReducerClass(MovieCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        // Set input and output paths for Job 1
        FileInputFormat.addInputPath(job1, new Path(args[0])); // Input: userID \t MovieName
        Path intermediateOutputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job1, intermediateOutputPath);

        // Delete intermediate output path if it already exists
        intermediateOutputPath.getFileSystem(conf1).delete(intermediateOutputPath, true);

        // Run Job 1 and wait for its completion
        boolean success = job1.waitForCompletion(true);
        if(!success){
            System.err.println("Job1 failed, exiting");
            System.exit(1);
        }

        // Configuration and Job setup for Job 2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Group Movies by Like-Count");
        job2.setJarByClass(LikeCountByMovie.class);
        job2.setMapperClass(CountMapper.class);
        job2.setReducerClass(GroupMoviesReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        // Set input and output paths for Job 2
        FileInputFormat.addInputPath(job2, intermediateOutputPath); // Input: MovieName \t LikeCount
        Path finalOutputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);

        // Delete final output path if it already exists
        finalOutputPath.getFileSystem(conf2).delete(finalOutputPath, true);

        // Run Job 2 and wait for its completion
        success = job2.waitForCompletion(true);
        if(!success){
            System.err.println("Job2 failed, exiting");
            System.exit(1);
        }

        System.exit(0);
    }
}
