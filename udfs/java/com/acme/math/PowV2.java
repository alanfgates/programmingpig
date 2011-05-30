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
package com.acme.math;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * A simple UDF that takes a value and raises it to the power of a second
 * value.  It can be used in a Pig Latin script as Pow(x, y), where x and y
 * are both expected to be ints.
 */
public class PowV2 extends EvalFunc<Double> {

    public Double exec(Tuple input) throws IOException {
System.out.println("in pow");
        try {
            return Math.pow((Double)input.get(0), (Double)input.get(1));
        } catch (Exception e) {
            throw new IOException("Something bad happened!", e);
        }
    }

    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.DOUBLE));
        s.add(new Schema.FieldSchema(null, DataType.DOUBLE));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.LONG));
        s.add(new Schema.FieldSchema(null, DataType.LONG));
        funcList.add(new FuncSpec(LongPow.class.getName(), s));
        return funcList;
    }


    public static class LongPow extends EvalFunc<Long> {

        public Long exec(Tuple input) throws IOException {
System.out.println("in longpow");
            try {
                long base = (Long)input.get(0);
                long exponent = (Long)input.get(1);
                long result = 1;

                for (long i = 0; i < exponent; i++) {
                    long preresult = result;
                    result *= base;
                    if (preresult > result) {
                        warn("Overflow!", PigWarning.TOO_LARGE_FOR_INT);
                        return null; 
                    }
                }
                return result;
            } catch (Exception e) {
                throw new IOException("Something bad happened!", e);
            }
        }
    }
}
