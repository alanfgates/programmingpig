/*
 * This code is made available under the Apache License, Version 2.0 (the
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

package com.acme.financial;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;

/**
 * This UDF takes a bag of information about a stock, and
 * produces a floating point score between 1 and 100, 
 * 1 being sell, 100 being buy.
 */
public class AnalyzeStockV2 extends EvalFunc<Float> {

	Random r = new Random();
    Properties myProperties = null;

    @Override
    public Float exec(Tuple input) throws IOException {
        if (myProperties == null) {
            // Retrieve our class specific properties from UDFContext
            myProperties =
                UDFContext.getUDFContext().getUDFProperties(this.getClass());
        }
            
		// Make sure the input isn't null and is of the right size.
		if (input == null || input.size() != 1) return null;

		DataBag b = (DataBag)input.get(0);
		for (Tuple t : b) {
			// Do some magic analysis, using properites from myProperties to
            // decide how ...
		}
		return r.nextFloat() * 100;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            // Read our properties file
            Properties prop = new Properties();
            prop.load(new FileInputStream("/tmp/stock.properties"));
            // Get a properties object specific to this UDF class
            UDFContext context = UDFContext.getUDFContext();
            Properties udfProp = context.getUDFProperties(this.getClass());
            // Copy our properties into it.  There is no need to pass it
            // back to UDFContext
            for (Map.Entry<Object, Object> e : prop.entrySet()) {
                udfProp.setProperty((String)e.getKey(), (String)e.getValue());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new Schema(new Schema.FieldSchema(null, DataType.FLOAT));
    }

}
