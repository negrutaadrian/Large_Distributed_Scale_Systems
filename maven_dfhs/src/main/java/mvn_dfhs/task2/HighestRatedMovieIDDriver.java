package mvn_dfhs.task2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import mvn_dfhs.task2.HighestRatedMovieName.RatingsMapper;
import mvn_dfhs.task2.HighestRatedMovieName.MaxRatingReducer;

// Driver
public class HighestRatedMovieIDDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: HighestRatedMovieIDDriver <ratings_input> <intermediate_output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Find Highest-Rated Movie ID per User");
        job1.setJarByClass(HighestRatedMovieName.class);

        job1.setMapperClass(RatingsMapper.class);
        job1.setReducerClass(MaxRatingReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0])); // ratings.csv
        Path intermediateOutputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job1, intermediateOutputPath);

        // Clean up the intermediate output directory if it exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(intermediateOutputPath)) {
            fs.delete(intermediateOutputPath, true);
        }

        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}