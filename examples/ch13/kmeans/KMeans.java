import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRExecType;
import org.apache.pig.data.Tuple;

public class KMeans {
    static final int MAX_SCORE = 4;
    static final int MIN_SCORE = 0;
    static final int MAX_ITERATION = 100;
    
    static public class FindCentroid extends EvalFunc<Double> {
        double[] centroids;
        public FindCentroid(String initialCentroid) {
            String[] centroidStrings = initialCentroid.split(":");
            centroids = new double[centroidStrings.length];
            for (int i=0;i<centroidStrings.length;i++)
                centroids[i] = Double.parseDouble(centroidStrings[i]);
        }
        @Override
        public Double exec(Tuple input) throws IOException {
            double min_distance = Double.MAX_VALUE;
            double closest_centroid = 0;
            for (double centroid : centroids) {
                double distance = Math.abs(centroid - (Double)input.get(0));
                if (distance < min_distance) {
                    min_distance = distance;
                    closest_centroid = centroid;
                }
            }
            return closest_centroid;
        }
    }

    static public void main(String[] args) throws IOException {

        int k = 4;
        double tolerance = 0.01;

        PigServer pig = new PigServer(new MRExecType());

        // initial centroid, equally divide the space
        String initial_centroids = "";
        double[] last_centroids = new double[k];
        for (int i=0;i<k;i++) {
            last_centroids[i] = MIN_SCORE + (double)i/k*(MAX_SCORE-MIN_SCORE);
            initial_centroids = initial_centroids + last_centroids[i];
            if (i!=k-1) {
                initial_centroids = initial_centroids + ":";
            }
        }

        boolean converged = false;
        int iter_num = 0;
        while (iter_num < MAX_ITERATION) {
            // Register Pig script. initial_centroids will be passed as a construct argument
            // and will be changed in every iteration. We calculate the average of the new clusters
            // to get the new centroid estimation
            pig.registerQuery("DEFINE find_centroid " + FindCentroid.class.getName() + "('" + initial_centroids + "');");
            pig.registerQuery("raw = load 'student.txt' as (name:chararray, age:int, gpa:double);");
            pig.registerQuery("centroided = foreach raw generate gpa, find_centroid(gpa) as centroid;");
            pig.registerQuery("grouped = group centroided by centroid;");
            pig.registerQuery("result = foreach grouped generate group, AVG(centroided.gpa);");

            // Get the new centroids from the output
            Iterator<Tuple> iter = pig.openIterator("result");
            double[] centroids = new double[k];
            double distance_move = 0;

            // Calculate the moving distance with last iteration
            for (int i=0;i<k;i++) {
                Tuple tuple = iter.next();
                centroids[i] = (double)tuple.get(1);
                distance_move = distance_move + Math.abs(last_centroids[i]-centroids[i]);
            }
            distance_move = distance_move / k;
            pig.deleteFile("output");
            System.out.println("iteration " + iter_num);
            System.out.println("average distance moved: " + distance_move);

            // Converge
            if (distance_move < tolerance) {
                System.out.println("k-means converged at centroids: ");
                System.out.println(Arrays.toString(centroids));
                converged = true;
                break;
            }

            // Not converge, use the new centroids as the initial centroids for next iteration
            last_centroids = centroids.clone();
            initial_centroids = "";
            for (int i=0;i<k;i++) {
                initial_centroids = initial_centroids + last_centroids[i];
                if (i!=k-1) {
                    initial_centroids = initial_centroids + ":";
                }
            }
            iter_num += 1;
        }
        // Not converge after MAX_ITERATION
        if (!converged) {
            System.out.println("not converge after " + iter_num + " iterations");
            System.out.println("last centroids: ");
            System.out.println(Arrays.toString(last_centroids));
        }
    }
}
