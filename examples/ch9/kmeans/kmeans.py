#!/usr/bin/python
# This code is made available under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.

import sys
from math import fabs
from org.apache.pig.scripting import Pig

@outputSchemaFunction("findCentroidSchema")
# Assign each value to the closest centroid
def findCentroid(initialCentroid, value):
    # initialCentroid is a constant, we can optimize to derive centroids only once
    centroids = initialCentroid.split(":")

    min_distance = float("inf")
    closest_centroid = 0
    for centroid in centroids:
        distance = fabs(float(centroid) - value)
        if distance < min_distance:
            min_distance = distance
            closest_centroid = centroid
    return float(closest_centroid)

# The output schema is the second field of the input, which is the same type of the param value
@schemaFunction("findCentroidSchema")
def findCentroidSchema(input):
    return input.getField(1)

def main():
    filename = "student.txt"
    k = 4
    tolerance = 0.01

    MAX_SCORE = 4
    MIN_SCORE = 0
    MAX_ITERATION = 100

    # initial centroid, equally divide the space
    initial_centroids = ""
    last_centroids = [None] * k
    for i in range(k):
        last_centroids[i] = MIN_SCORE + float(i)/k*(MAX_SCORE-MIN_SCORE)
        initial_centroids = initial_centroids + str(last_centroids[i])
        if i!=k-1:
            initial_centroids = initial_centroids + ":"

    # Compile Pig script. Register the same script since it contains the Jython UDF.
    # $centroids is the only binding parameter. It will be bound to different parameter with the
    # estimation for centroid from the last round. Then we calculate the average of the new clusters
    # to get the new centroid estimation, and store into "output"
    P = Pig.compile("""register 'kmeans.py' using jython as util;
                       raw = load 'student.txt' as (name:chararray, age:int, gpa:double);
                       centroided = foreach raw generate gpa, util.findCentroid('$centroids', gpa) as centroid;
                       grouped = group centroided by centroid;
                       result = foreach grouped generate group, AVG(centroided.gpa);
                       store result into 'output';
                    """)

    converged = False
    iter_num = 0
    while iter_num < MAX_ITERATION:
        # Binding parameter centroids to current centroids
        Q = P.bind({'centroids':initial_centroids})

        # Run Pig script
        results = Q.runSingle()

        # Check the result of the Pig script
        if results.isSuccessful() == "FAILED":
            raise "Pig job failed"

        # Get the new centroids from the output
        iter = results.result("result").iterator()
        centroids = [None] * k
        distance_move = 0

        # Calculate the moving distance with last iteration
        for i in range(k):
            tuple = iter.next()
            centroids[i] = float(str(tuple.get(1)))
            distance_move = distance_move + fabs(last_centroids[i]-centroids[i])
        distance_move = distance_move / k;
        Pig.fs("rmr output")
        print("iteration " + str(iter_num))
        print("average distance moved: " + str(distance_move))

        # Converge
        if distance_move < tolerance:
            sys.stdout.write("k-means converged at centroids: [")
            sys.stdout.write(",".join(str(v) for v in centroids))
            sys.stdout.write("]\n")
            converged = True
            break

        # Not converge, use the new centroids as the initial centroids for next iteration
        last_centroids = centroids[:]
        initial_centroids = ""
        for i in range(k):
            initial_centroids = initial_centroids + str(last_centroids[i])
            if i!=k-1:
                initial_centroids = initial_centroids + ":"
        iter_num += 1

    # Not converge after MAX_ITERATION
    if not converged:
        print("not converge after " + str(iter_num) + " iterations")
        sys.stdout.write("last centroids: [")
        sys.stdout.write(",".join(str(v) for v in last_centroids))
        sys.stdout.write("]\n")

if __name__ == "__main__":
    main()
