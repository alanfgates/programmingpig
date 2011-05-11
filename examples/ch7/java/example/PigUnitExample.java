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
package example;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.BeforeClass;
import org.junit.Test;

public class PigUnitExample {
    private PigTest test;
    private static Cluster cluster;

    @Test
    public void testDataInFile() throws ParseException, IOException {
        // Construct an instance of PigTest that will use the script
        // pigunit.pig
        test = new PigTest("../pigunit.pig");

        // Specify our expected output.  The format is a string for each line.
        // In this particular case we only expect one line of output.
        String[] output = { "(0.27305267014925455)" };

        // Run the test and check that the output matches our expectation.
        // The "avgdiv" tells PigUnit what alias to check the output value
        // against.  It inserts a store for that alias and then checks the 
        // contents of the stored file against output
        test.assertOutput("avgdiv", output);
    }

    @Test
    public void testTextInput() throws ParseException, IOException  {
        test = new PigTest("../pigunit.pig");

        // Rather than read from a file, generate synthetic input.
        // Format is one record per line, tab separated.
        String[] input = {
            "NYSE\tCPO\t2009-12-30\t0.14",
            "NYSE\tCPO\t2009-01-06\t0.14",
            "NYSE\tCCS\t2009-10-28\t0.414",
            "NYSE\tCCS\t2009-01-28\t0.414",
            "NYSE\tCIF\t2009-12-09\t0.029",
        };

        String[] output = { "(0.22739999999999996)" };

        // Run the example script using the input we constructed
        // rather than loading whatever the load statement says.
        // "divs" is the alias to override with the input data
        // As with the previous example "avgdiv" is the alias
        // to test against the value(s) in output.
        test.assertOutput("divs", input, "avgdiv", output);
    }

    @Test
    public void testFileOutput() throws ParseException, IOException {
        // The script as an array of strings, one line per string.
          String[] script = {
            "divs   = load '../../../data/NYSE_dividends' as (exchange, symbol, date, dividends);",
            "grpd   = group divs all;",
            "avgdiv = foreach grpd generate AVG(divs.dividends);",
            "store avgdiv into 'average_dividend';",
        };
        test = new PigTest(script);

        // Test output against an existing file that contains the
        // expected output.
        test.assertOutput(new File("../expected.out"));
    }

    @Test
    public void testWithParams() throws ParseException, IOException {
        // Parameters to be substituted in Pig Latin script before the 
        // test is run.  Format is one string for each parameter,
        // parameter=value
        String[] params = { 
            "input=../../../data/NYSE_dividends",
            "output=average_dividend2"
        };
        test = new PigTest("../pigunitwithparams.pig", params);

        String[] output = { "(0.27305267014925455)" };

        // Test output in stored file against specified result
        test.assertOutput(output);
    }
}
