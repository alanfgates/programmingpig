// This code is made available under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

class GroovyUDF {
    @OutputSchema("m:[chararray]")
    static Map<String, String> constructMapFromBag(groovy.lang.Tuple bag, String value) {
        Map<String, String> output = new HashMap<String, String>();
        Iterator<groovy.lang.Tuple> iter = bag.get(1);
        while (iter.hasNext()) {
            output.put(iter.next().get(0), value);
        }
        return output;
    }

    @OutputSchema("keys:{(key:chararray)}")
    static DataBag getAllMapKeys(Map<String, String> m) {
        DataBag output = DefaultBagFactory.getInstance().newDefaultBag();
        for (String key : m.keySet()) {
            Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
            t.set(0, key);
            output.add(t);
        }
        return output;
    }
}
