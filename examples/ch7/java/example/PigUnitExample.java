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
    test = new PigTest("../pigunit.pig");

    String[] output = { "(0.27305267014925455)" };

    test.assertOutput("avgdiv", output);
  }

  @Test
  public void testTextInput() throws ParseException, IOException  {
    test = new PigTest("../pigunit.pig");

    String[] input = {
		"NYSE\tCPO\t2009-12-30\t0.14",
		"NYSE\tCPO\t2009-01-06\t0.14",
		"NYSE\tCCS\t2009-10-28\t0.414",
		"NYSE\tCCS\t2009-01-28\t0.414",
		"NYSE\tCIF\t2009-12-09\t0.029",
    };

    String[] output = { "(0.22739999999999996)" };

    test.assertOutput("divs", input, "avgdiv", output);
  }

}
