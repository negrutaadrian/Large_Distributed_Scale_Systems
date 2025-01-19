package mvn_dfhs.task2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import mvn_dfhs.task2.HighestRatedMovieName.UserMaxMovieMapper;
import mvn_dfhs.task2.HighestRatedMovieName.MoviesMapper;

public class JoinMovieNamesDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: JoinMovieNamesDriver <intermediate_output> <movies_input> <final_output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job2 = Job.getInstance(conf, "Join Highest-Rated Movie IDs with Movie Names");
        job2.setJarByClass(HighestRatedMovieName.class);

        // Use MultipleInputs to add both datasets with their respective mappers
        MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class, UserMaxMovieMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, MoviesMapper.class);

        job2.setReducerClass(HighestRatedMovieName.JoinReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        Path finalOutputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);

        // Clean up the final output directory if it exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(finalOutputPath)) {
            fs.delete(finalOutputPath, true);
        }

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
