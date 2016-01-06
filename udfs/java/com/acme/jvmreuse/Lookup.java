/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.acme.jvmreuse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.JVMReuseManager;
import org.apache.pig.StaticDataCleanup;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.util.UDFContext;

public class Lookup extends EvalFunc<Integer> {
    String filename = null;
    static Set<String> mKeys = null; 

    // register cleanup hook
    static {
        JVMReuseManager.getInstance().registerForStaticDataCleanup(Lookup.class);
    }

    // clean up static variable
    @StaticDataCleanup
    public static void staticDataCleanup() {
        mKeys = null;
    }
    public Lookup(String filename) {
        this.filename = filename;
    }

    public void init() throws IOException {
        mKeys = new HashSet<String>();
        Properties props = ConfigurationUtil.toProperties(
                UDFContext.getUDFContext().getJobConf());
        BufferedReader reader = null;
        InputStream is = null;
        try {
            is = FileLocalizer.openDFSFile(filename, props);
        } catch (IOException e) {
            String msg = "Lookup : Cannot open file " + filename;
            throw new IOException(msg, e);
        }
        try {
            reader = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = reader.readLine()) != null) {
                mKeys.add(line);
            }
            is.close();
        } catch (IOException e) {
            String msg = "Lookup : Cannot read file " + filename;
            throw new IOException(msg, e);
        }
    }

    @Override
    public Integer exec(Tuple input) throws IOException {
        if (mKeys == null) {
            init();
        }
        if (mKeys.contains(input.get(0).toString()))
            return 1;
        return 0;
    }
}
