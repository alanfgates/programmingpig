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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

/**
 * A lookup UDF that maps cities to metropolatin areas.
 */
public class MetroResolver extends EvalFunc<String> {

    String lookupFile;
    HashMap<String, String> lookup = null;

    /*
     * @param file - File that contains a lookup table mapping cities to metro
     * areas.  The file must be located on the file system where this UDF will
     * run.
     */
    public MetroResolver(String file) {
        // Just store the filename, don't load the lookup table since we may
        // be on the front end or the back end.
        lookupFile = file;
    }

    public String exec(Tuple input) throws IOException {
        if (lookup == null) {
            // We have not been initialized yet, so do it now.
            lookup = new HashMap<String, String>();

            // Get an instance of the HDFS FileSystem class so
            // we can read a file from HDFS.  We need a copy of
            // our configuration to do that.
            // Read the configuration from the UDFContext
            FileSystem fs =
                FileSystem.get(UDFContext.getUDFContext().getJobConf());
            DataInputStream in = fs.open(new Path(lookupFile));
            String line;
            while ((line = in.readLine()) != null) {
                String[] toks = new String[2];
                toks = line.split(":", 2);
                lookup.put(toks[0], toks[1]);
            }
            in.close();
        }
        return lookup.get((String)input.get(0));
    }
}



