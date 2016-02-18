// This code is made available under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

import org.apache.pig.scripting.Pig;
import org.apache.pig.scripting.groovy.OutputSchemaFunction;

class Kmeans {
    @OutputSchemaFunction('findCentroidSchema')
    // Assign each value to the closest centroid
    public static findCentroid(initialCentroid, value) {
        // initialCentroid is a constant, we can optimize to derive centroids only once
        def centroids = initialCentroid.split(":");

        def min_distance = Double.MAX_VALUE;
        def closest_centroid = 0;
        centroids.each { centroid ->
            def distance = Math.abs(Double.parseDouble(centroid) - value);
            if (distance < min_distance) {
                min_distance = distance;
                closest_centroid = centroid;
            }
        }
        return Double.parseDouble(closest_centroid);
    }

    // The output schema is the second field of the input, which is the same type of the param value
    public static findCentroidSchema(input) {          
        return input[1];
    }

    public static void main(String[] args) {
        def filename = "student.txt";
        def k = 4;
        def tolerance = 0.01;

        def MAX_SCORE = 4;
        def MIN_SCORE = 0;
        def MAX_ITERATION = 100;

        // initial centroid, equally divide the space
        def initial_centroids = "";
        def last_centroids = new double[k];
        for (def i=0;i<k;i++) {
            last_centroids[i] = MIN_SCORE + (double)i/k*(MAX_SCORE-MIN_SCORE);
            initial_centroids = initial_centroids + last_centroids[i].toString();
            if (i!=k-1) {
                initial_centroids = initial_centroids + ":"
            }
        }

        // Compile Pig script. Register the same script since it contains the Jython UDF.
        // $centroids is the only binding parameter. It will be bound to different parameter with the
        // estimation for centroid from the last round. Then we calculate the average of the new clusters
        // to get the new centroid estimation, and store into "output"
        def P = Pig.compile("register './kmeans.groovy' using groovy as util;" +
                            "raw = load 'student.txt' as (name:chararray, age:int, gpa:double);" +
                            "centroided = foreach raw generate gpa, util.findCentroid('\$centroids', gpa) as centroid;" +
                            "grouped = group centroided by centroid;" +
                            "result = foreach grouped generate group, AVG(centroided.gpa);" +
                            "store result into 'output';");

        def converged = false;
        def iter_num = 0;
        while (iter_num < MAX_ITERATION) {
            // Binding parameter centroids to current centroids
            def Q = P.bind(['centroids':initial_centroids]);

            // Run Pig script
            def results = Q.runSingle();

            // Check the result of the Pig script
            if (results.isSuccessful() == "FAILED") {
                throw new RuntimeException("Pig job failed");
            }

            // Get the new centroids from the output
            def iter = results.result("result").iterator();
            def centroids = new double[k];
            def distance_move = 0;

            // Calculate the moving distance with last iteration
            for (def i=0;i<k;i++) {
                def tuple = iter.next();
                centroids[i] = Double.parseDouble(tuple.get(1).toString());
                distance_move = distance_move + Math.abs(last_centroids[i]-centroids[i]);
            }
            distance_move = distance_move / k;
            Pig.fs("rmr output");
            print("iteration " + iter_num.toString() + "\n");
            print("average distance moved: " + distance_move.toString() + "\n");

            // Converge
            if (distance_move < tolerance) {
                print("k-means converged at centroids: [");
                print(centroids.iterator().join(","));
                print("]\n");
                converged = true;
                break;
            }

            // Not converge, use the new centroids as the initial centroids for next iteration
            last_centroids = centroids.clone();
            initial_centroids = "";
            for (def i=0;i<k;i++) {
                initial_centroids = initial_centroids + last_centroids[i].toString();
                if (i!=k-1) {
                    initial_centroids = initial_centroids + ":";
                }
            }
            iter_num += 1;
        }

        // Not converge after MAX_ITERATION
        if (!converged) {
            print("not converge after " + str(iter_num) + " iterations");
            print("last centroids: [");
            print(last_centroids.iterator().join(","));
            print("]\n");
        }
    }
}
