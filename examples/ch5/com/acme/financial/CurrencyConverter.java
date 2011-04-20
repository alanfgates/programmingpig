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

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class CurrencyConverter extends EvalFunc<Float> {

	String from, to;

	public CurrencyConverter(String from, String to) {
		super();
		this.from = from;
		this.to = to;
	}

    @Override
    public Float exec(Tuple input) throws IOException {
		// Make sure the input isn't null and is of the right size.
		if (input == null || input.size() != 1) return null;
		// do some magic lookup in a table
		// ...
		return (Float)input.get(0) * 1.5f;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.FLOAT));
    }

}
