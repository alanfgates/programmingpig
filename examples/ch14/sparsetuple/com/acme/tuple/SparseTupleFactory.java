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

import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.TupleRawComparator;

public class SparseTupleFactory extends TupleFactory {

    @Override
    public Tuple newTuple() {
        return new SparseTuple();
    }

    @Override
    public Tuple newTuple(int size) {
        return new SparseTuple(size);
    }

    // Put the non-null fields into SparseTuple
    @Override
    public Tuple newTuple(List c) {
        Tuple t = new SparseTuple(c.size());
        try {
            for (int i=0;i<c.size();i++) {
                if (c.get(i) != null) {
                    t.set(i, c.get(i));
                }
            }
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
        return t;
    }

    // In SparseTuple, this makes no difference than newTuple.
    // Since the underlining data structure is not list, we need
    // to do a copy into TreeMap anyway
    @Override
    public Tuple newTupleNoCopy(List list) {
        return newTuple(list);
    }

    @Override
    public Tuple newTuple(Object datum) {
        Tuple t = new SparseTuple(1);
        t.append(datum);
        return t;
    }

    @Override
    public Class<? extends Tuple> tupleClass() {
        return SparseTuple.class;
    }

    // This is the comparator for the serialized data. Since SparseTuple is
    // only a memory data structure and use BinSedesTuple as the serialized form,
    // we use the comparator of BinSedesTuple
    @Override
    public Class<? extends TupleRawComparator> tupleRawComparatorClass() {
        return BinSedesTuple.getComparatorClass();
    }

    @Override
    public boolean isFixedSize() {
        return false;
    }
}
