package mvn_dfhs.task2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * Two-stage MapReduce to find the highest-rated movie name per userID.
 */
public class HighestRatedMovieName {

    // Job 1: Mapper for ratings.csv
    public static class RatingsMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();

            // Skip header
            if (key.get() == 0 && line.toLowerCase().contains("userid")) {
                context.getCounter("RatingsMapper", "HeaderSkipped").increment(1);
                return;
            }

            String[] fields = line.split(",");
            if (fields.length < 4) {
                context.getCounter("RatingsMapper", "InvalidLines").increment(1);
                return;
            }

            String userID = fields[0].trim();
            String movieID = fields[1].trim();
            String ratingStr = fields[2].trim();

            try {
                double rating = Double.parseDouble(ratingStr); // Validate rating
                context.write(new Text(userID), new Text(movieID + "," + rating));
                context.getCounter("RatingsMapper", "ValidLines").increment(1);
            } catch (NumberFormatException e) {
                context.getCounter("RatingsMapper", "InvalidRatings").increment(1);
            }
        }
    }


    // Job 1: Reducer to find the highest-rated movie ID for each userID
    public static class MaxRatingReducer extends Reducer<Text, Text, Text, Text> {
        private final Random random = new Random();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String maxMovieID = null;
            double maxRating = Double.MIN_VALUE;

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                if (parts.length < 2) {
                    context.getCounter("MaxRatingReducer", "InvalidValues").increment(1);
                    continue;
                }

                String movieID = parts[0].trim();
                String ratingStr = parts[1].trim();

                try {
                    double rating = Double.parseDouble(ratingStr);
                    if (rating > maxRating) {
                        maxRating = rating;
                        maxMovieID = movieID;
                    }
                } catch (NumberFormatException e) {
                    context.getCounter("MaxRatingReducer", "InvalidRatings").increment(1);
                }
            }

            if (random.nextInt(10) < 2) {
                System.out.println("this was easy");
            }

            if (maxMovieID != null) {
                context.write(key, new Text(maxMovieID));
                context.getCounter("MaxRatingReducer", "OutputRecords").increment(1);
            } else {
                context.getCounter("MaxRatingReducer", "NoMaxFound").increment(1);
            }
        }
    }



    /*
    userID, movieID
    1, 8542
     */

    /* =============== Job 2 =============== */

    // Job 2: Mapper for movies.csv
    public static class MoviesMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();

            // Skip header
            if (key.get() == 0 && line.toLowerCase().contains("movieid")) {
                context.getCounter("MoviesMapper", "HeaderSkipped").increment(1);
                return;
            }

            // Split only on the first comma to handle movie names with commas
            String[] fields = line.split(",", 2);
            if (fields.length < 2) {
                context.getCounter("MoviesMapper", "InvalidLines").increment(1);
                return;
            }

            String movieID = fields[0].trim();
            String movieName = fields[1].trim();

            context.write(new Text(movieID), new Text("M:" + movieName));
            context.getCounter("MoviesMapper", "ValidLines").increment(1);
        }
    }


    // Job 2: Mapper for the output of Job 1 (highest-rated movies per user)
    public static class UserMaxMovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 2) {
                context.getCounter("UserMaxMovieMapper", "InvalidLines").increment(1);
                return;
            }

            String userID = fields[0].trim();
            String movieID = fields[1].trim();

            context.write(new Text(movieID), new Text("U:" + userID));
            context.getCounter("UserMaxMovieMapper", "ValidLines").increment(1);
        }
    }


    // Job 2: Reducer to join userID and movieName
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieName = null;
            List<String> userIDs = new ArrayList<>();

            for (Text value : values) {
                String val = value.toString();
                if (val.startsWith("M:")) {
                    movieName = val.substring(2).trim();
                } else if (val.startsWith("U:")) {
                    userIDs.add(val.substring(2).trim());
                }
            }

            if (movieName != null && !userIDs.isEmpty()) {
                for (String userID : userIDs) {
                    context.write(new Text(userID), new Text(movieName));
                    context.getCounter("JoinReducer", "OutputRecords").increment(1);
                }
            } else {
                context.getCounter("JoinReducer", "NoJoin").increment(1);
            }
        }
    }
}
