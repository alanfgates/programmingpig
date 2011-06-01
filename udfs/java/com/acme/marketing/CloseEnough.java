/**
 * This code is made available under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.acme.marketing;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/**
 * A filter UDF that determines if two zip codes are within a given distance.
 */
public class CloseEnough extends FilterFunc {

    int distance;
    Random r = new Random();

    /*
     * @param miles - Distance in miles that two zip codes can be apart and
     * still be considered close enough.
     */
    public CloseEnough(String miles) {
        // UDFs can only takes strings; convert to int here
        distance = Integer.valueOf(miles);
    }

    public Boolean exec(Tuple input) throws IOException {
        // expect two strings
        String zip1 = (String)input.get(0);
        String zip2 = (String)input.get(1);
        // do some lookup on zip code tables
        return r.nextBoolean();
    }
}



