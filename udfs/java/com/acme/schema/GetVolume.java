package com.acme.schema;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class GetVolume extends EvalFunc<Integer> {
    @Override
    public Integer exec(Tuple input) throws IOException {
        Schema inputSchema = getInputSchema();
        int pos = inputSchema.getFields().indexOf(inputSchema.getField("volume"));
        if (pos == -1) {
            return -1;
        }
        return (Integer)input.get(pos);
    }
}
