package com.acme.intersectexcept;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class ReplicateRows extends EvalFunc<DataBag> {
    int numRelations = -1;
    static BagFactory bagFactory = BagFactory.getInstance();
    static TupleFactory tupleFactory = TupleFactory.getInstance();
    enum Mode {INTERSECT, INTERSECT_ALL, EXCEPT, EXCEPT_ALL};
    Mode mode;
    public ReplicateRows(String mode) {
        this.mode = Mode.valueOf(mode);
    }
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (numRelations == -1) {
            numRelations = input.size();
        }
        Map<Integer, Long> counts = new HashMap<Integer, Long>();
        for (int i=0;i<numRelations;i++) {
            DataBag inputBag = (DataBag)input.get(i);
            Iterator<Tuple> iter = inputBag.iterator();
            if (iter.hasNext()) {
                counts.put(i, (Long)iter.next().get(1));
            } else {
                counts.put(i, 0L);
            }
        }
        long outputCount = 0;
        switch (mode) {
        case INTERSECT:
            outputCount = 1;
            for (int i=0;i<counts.size();i++) {
                if (counts.get(i) == 0) {
                    outputCount = 0;
                    break;
                }
            }
            break;            
        case INTERSECT_ALL:
            outputCount = Long.MAX_VALUE;
            for (int i=0;i<counts.size();i++) {
                if (counts.get(i) < outputCount) {
                    outputCount = counts.get(i);
                }
            }
            break;
        case EXCEPT:
            if (counts.get(0) != 0) {
                outputCount = 1;
                for (int i=1;i<counts.size();i++) {
                    if (counts.get(i) != 0) {
                        outputCount = 0;
                        break;
                    }
                }
                break;
            }
        case EXCEPT_ALL:
            outputCount = counts.get(0);
            for (int i=1;i<counts.size();i++) {
                outputCount -= counts.get(i);
            }
            break;
        default:
            throw new IOException("Unknown execution mode");
        }
        DataBag outputBag = bagFactory.newDefaultBag();
        for (int i=0;i<outputCount;i++) {
            Tuple t = tupleFactory.newTuple();
            outputBag.add(t);
        }
        return outputBag;
    }

}
