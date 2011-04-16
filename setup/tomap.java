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

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class tomap extends EvalFunc<Map> {

	private Tuple current = null;
	private Map<String, Object> output = null;

    @Override
    public Map exec(Tuple input) throws IOException {
		// Make sure the input isn't null and is of the rigth size.
		if (input == null || input.size() != 25) return null;
		current = input;
    	try {
	    	output = new HashMap<String, Object>();

			// for each field we're interested in, only put it in
			// the output if it is non-null
			// ignore name, id, player, starting_season, ending_season
			ifNotNull("games", 5);
			ifNotNull("at_bats", 6);
			ifNotNull("hits", 7);
			ifNotNull("runs", 8);
			ifNotNull("doubles", 9);
			ifNotNull("triples", 10);
			ifNotNull("home_runs", 11);
			ifNotNull("grand_slams", 12);
			ifNotNull("rbis", 13);
			ifNotNull("base_on_balls", 14);
			ifNotNull("ibbs", 15);
			ifNotNull("strikeouts", 16);
			ifNotNull("sacrifice_hits", 17);
			ifNotNull("sacrifice_flies", 18);
			ifNotNull("hit_by_pitch", 19);
			ifNotNull("gdb", 20);
			ifNotNull("batting_average", 21);
			ifNotNull("on_base_percentage", 22);
			ifNotNull("slugging_percentage", 23);
	
	    	return output;
		} catch (ArrayIndexOutOfBoundsException e){
			throw new RuntimeException("Function input must have even number of parameters");
        } catch (Exception e) {
            throw new RuntimeException("Error while creating a map", e);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.MAP));
    }

	private void ifNotNull(String key, int pos) throws ExecException{
		Object o = current.get(pos);
		if (o == null || "0".equals(o.toString()) || "0.0".equals(o.toString())) return;
		output.put(key, o);
	}

}
