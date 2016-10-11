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

importPackage(Packages.org.apache.pig.scripting.js)
pig = org.apache.pig.scripting.js.JSPig;

// JavaScript UDF does not support output schema function, has to use fixed schema
findCentroid.outputSchema = "value:double";
// Assign each value to the closest centroid
function findCentroid(initialCentroid, value) {
    // initialCentroid is a constant, we can optimize to derive centroids only once
    var centroids = initialCentroid.split(":");

    var min_distance = Infinity;
    var closest_centroid = 0;
    centroids.forEach(function(centroid) {
        distance = Math.abs(Number(centroid) - value);
        if (distance < min_distance) {
            min_distance = distance;
            closest_centroid = centroid;
        }
    });
    return Number(closest_centroid);
}

function main() {
    var filename = "student.txt";
    var k = 4;
    var tolerance = 0.01;

    var MAX_SCORE = 4;
    var MIN_SCORE = 0;
    var MAX_ITERATION = 100;

    // initial centroid, equally divide the space
    var initial_centroids = "";
    var last_centroids = new Array(k);
    for (var i=0;i<k;i++) {
        last_centroids[i] = MIN_SCORE + Number(i)/k*(MAX_SCORE-MIN_SCORE);
        initial_centroids = initial_centroids + last_centroids[i].toString();
        if (i!=k-1) {
            initial_centroids = initial_centroids + ":"
        }
    }

    // Compile Pig script. Register the same script since it contains the Jython UDF.
    // $centroids is the only binding parameter. It will be bound to different parameter with the
    // estimation for centroid from the last round. Then we calculate the average of the new clusters
    // to get the new centroid estimation, and store into "output"
    var P = pig.compile("register 'kmeans.js' using javascript as util;" +
                        "raw = load 'student.txt' as (name:chararray, age:int, gpa:double);" +
                        "centroided = foreach raw generate gpa, util.findCentroid('$centroids', gpa) as centroid;" +
                        "grouped = group centroided by centroid;" +
                        "result = foreach grouped generate group, AVG(centroided.gpa);" +
                        "store result into 'output';");

    var converged = false;
    var iter_num = 0;
    while (iter_num < MAX_ITERATION) {
        // Binding parameter centroids to current centroids
        var Q = P.bind({'centroids':initial_centroids});

        // Run Pig script
        var results = Q.runSingle();

        // Check the result of the Pig script
        if (results.isSuccessful() == "FAILED") {
            throw "Pig job failed";
        }

        // Get the new centroids from the output
        var iter = results.result("result").iterator();
        var centroids = new Array(k);
        var distance_move = 0;

        // Calculate the moving distance with last iteration
        for (i=0;i<k;i++) {
            var tuple = iter.next();
            centroids[i] = Number(tuple.get(1).toString());
            distance_move = distance_move + Math.abs(last_centroids[i]-centroids[i]);
        }
        distance_move = distance_move / k;
        pig.fs("rmr output");
        print("iteration " + iter_num.toString() + "\n");
        print("average distance moved: " + distance_move.toString() + "\n");

        // Converge
        if (distance_move < tolerance) {
            print("k-means converged at centroids: [");
            print(centroids.join(","));
            print("]\n");
            converged = true;
            break;
        }

        // Not converge, use the new centroids as the initial centroids for next iteration
        last_centroids = centroids.slice();
        initial_centroids = "";
        for (i=0;i<k;i++) {
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
        print(last_centroids.join(","));
        print("]\n");
    }
}
