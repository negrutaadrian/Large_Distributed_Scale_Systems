package mvn_dfhs.task1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Custom Writable class to hold (movieID, rating)
class MovieRatingWritable implements Writable {
    private String movieID;
    private double rating;

    // Default constructor
    public MovieRatingWritable() {}

    public MovieRatingWritable(String movieID, double rating) {
        this.movieID = movieID;
        this.rating = rating;
    }

    public String getMovieID() {
        return movieID;
    }

    public double getRating() {
        return rating;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(movieID);
        out.writeDouble(rating);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.movieID = in.readUTF();
        this.rating = in.readDouble();
    }

    @Override
    public String toString() {
        return movieID + ":" + rating;
    }
}

public class HighestRatedMoviePerUser {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Highest Rated Movie Per User");

        job.setJarByClass(HighestRatedMoviePerUser.class);
        job.setMapperClass(HighestRatedMoviePerUserMapper.class);
        job.setReducerClass(HighestRatedMoviePerUserReducer.class);

        job.setMapOutputKeyClass(Text.class); // userID as key
        job.setMapOutputValueClass(MovieRatingWritable.class); // Custom Writable for (movieID, rating)

        job.setOutputKeyClass(Text.class); // userID
        job.setOutputValueClass(Text.class); // movieID (highest-rated)

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Mapper Class
    public static class HighestRatedMoviePerUserMapper extends Mapper<LongWritable, Text, Text, MovieRatingWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the header line
            if (key.get() == 0) return;

            String line = value.toString();
            String[] fields = line.split(",");

            // Input format: userID, movieID, rating
            if (fields.length >= 3) {
                String userID = fields[0].trim();
                String movieID = fields[1].trim();
                double rating = Double.parseDouble(fields[2].trim());

                // Emit (userID, (movieID, rating))
                context.write(new Text(userID), new MovieRatingWritable(movieID, rating));
            }
        }
    }

    // Reducer Class
    public static class HighestRatedMoviePerUserReducer extends Reducer<Text, MovieRatingWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<MovieRatingWritable> values, Context context)
                throws IOException, InterruptedException {
            String highestRatedMovie = null;
            double maxRating = Double.MIN_VALUE;

            // Iterate through all movie ratings for this user
            for (MovieRatingWritable value : values) {
                if (value.getRating() > maxRating) {
                    maxRating = value.getRating();
                    highestRatedMovie = value.getMovieID();
                }
            }

            // Emit (userID, highestRatedMovie - MovieID)
            if (highestRatedMovie != null) {
                context.write(key, new Text(highestRatedMovie));
            }
        }
    }
}
