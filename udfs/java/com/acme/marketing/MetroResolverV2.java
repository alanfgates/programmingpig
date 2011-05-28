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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * A lookup UDF that maps cities to metropolatin areas.  This time using the
 * Distributed Cache.
 */
public class MetroResolverV2 extends EvalFunc<String> {

    String lookupFile;
    HashMap<String, String> lookup = null;

    /*
     * @param file - File that contains a lookup table mapping cities to metro
     * areas.  The file must be located on the file system where this UDF will
     * run.
     */
    public MetroResolverV2(String file) {
        // Just store the filename, don't load the lookup table since we may
        // be on the front end or the back end.
        lookupFile = file;
    }

    public String exec(Tuple input) throws IOException {
        if (lookup == null) {
            // We have not been initialized yet, so do it now.
            lookup = new HashMap<String, String>();

            // Open the file as a local file
            FileReader fr = new FileReader("./mrv2_lookup");
            BufferedReader d = new BufferedReader(fr);
            String line;
            while ((line = d.readLine()) != null) {
                String[] toks = new String[2];
                toks = line.split(":", 2);
                lookup.put(toks[0], toks[1]);
            }
            fr.close();
        }
        return lookup.get((String)input.get(0));
    }

    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        // We were passed the name of the file on HDFS.  Append a
        // name for the file on the task node.
        list.add(lookupFile + "#mrv2_lookup");
        return list;
    }
}



