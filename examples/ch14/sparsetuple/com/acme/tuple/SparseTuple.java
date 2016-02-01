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
package com.acme.tuple;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.AbstractTuple;
import org.apache.pig.data.BinInterSedes;
import org.apache.pig.data.DataType;
import org.apache.pig.data.SizeUtil;
import org.apache.pig.data.Tuple;

public class SparseTuple extends AbstractTuple {

    private static final long serialVersionUID = 1L;

    int size;
    // Use TreeMap here so the index is sorted. This will help compare two SparseTuple
    Map<Integer, Object> matrix = new TreeMap<Integer, Object>();

    // BinInterSedes is used to serialize SparseTuple. We only use
    // SparseTuple as memory data structure. It will serialized into
    // BinSedesTuple if cross map/reduce boundary since Pig don't provide
    // a way to customize the serialized format. Even if you use custom
    // tuple implementation, when Pig deserialize it, Pig assumes this is
    // BinSedesTuple format
    private static final BinInterSedes bis = new BinInterSedes();

    public SparseTuple() {
    }

    public SparseTuple(int size) {
        this.size = size;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Object get(int fieldNum) throws ExecException {
        return matrix.get(fieldNum);
    }

    // flatten size/matrix into a List
    @Override
    public List<Object> getAll() {
        List<Object> result = new ArrayList<Object>(size);
        for (int i=0;i<=size;i++) {
            result.add(null);
        }
        for (Map.Entry<Integer, Object> entry : matrix.entrySet()) {
            result.set(entry.getKey(), entry.getValue());
        }
        return result;
    }

    @Override
    public void set(int fieldNum, Object val) throws ExecException {
        if (val == null) {
            return;
        }
        if (fieldNum >= size) {
            throw new ExecException("Index out of boundary. Try to access field " + fieldNum +
                    ", but the size of the tuple is " + size);
        }
        matrix.put(fieldNum, val);
    }

    @Override
    public void append(Object val) {
        if (val != null) {
            matrix.put(size, val);
        }
        size++;
    }

    // Memory estimate for SparseTuple
    @Override
    public long getMemorySize() {
        // Every entry in TreeMap takes 40 bytes, size is integer which takes 4 bytes
        long memorySize = 40 * matrix.size() + 4;
        for (Map.Entry<Integer, Object> entry : matrix.entrySet()) {
            // Every Integer object takes 16 bytes
            memorySize += 16;
            // Get the size of the value
            memorySize += SizeUtil.getPigObjMemSize(entry.getValue());
        }
        return memorySize;
    }

    // Use BinInterSedes to serialize so BinSedesTuple can read it back
    @Override
    public void write(DataOutput out) throws IOException {
        bis.writeDatum(out, this);
    }

    // Use BinSedesTuple to deserialize
    @Override
    public void readFields(DataInput in) throws IOException {
        Tuple t = (Tuple)bis.readDatum(in);
        size = t.size();
        for (int i=0;i<size;i++) {
            set(i, t.get(i));
        }
    }

    // Compare two SparseTuple. We can compare entry by entry since
    // index are sorted
    @Override
    public int compareTo(Object other) {
        if (other instanceof SparseTuple) {
            SparseTuple t = (SparseTuple) other;
            int mySz = size();
            int tSz = t.size();
            if (tSz < mySz) {
                return 1;
            } else if (tSz > mySz) {
                return -1;
            } else {
                Iterator<Map.Entry<Integer, Object>> otherEntryIter = t.matrix.entrySet().iterator();
                for (Map.Entry<Integer, Object> entry : matrix.entrySet()) {
                    Map.Entry<Integer, Object> otherEntry = otherEntryIter.next();
                    int c = entry.getKey() - otherEntry.getKey();
                    if (c != 0) {
                        return c;
                    }
                    c = DataType.compare(entry.getValue(), otherEntry.getValue());
                    if (c != 0) {
                        return c;
                    }
                }
                return 0;
            }
        } else {
            return DataType.compare(this, other);
        }
    }

    // Use the same way as DefaultTuple to do the hash. This method is necessary
    // since it will be used to test the equality of two SparseTuple
    @Override
    public int hashCode() {
        int hash = 17;
        for (Map.Entry<Integer, Object> entry : matrix.entrySet()) {
            if (entry.getValue() != null) {
                hash = 31 * hash + entry.getValue().hashCode();
            }
        }
        return hash;
    }
}
