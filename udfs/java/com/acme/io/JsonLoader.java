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
package com.acme.io;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.pig.Expression;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.LoadPushDown.OperatorSet;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

/**
 * A loader for data stored using {@link JsonStorage}.  This is not a generic
 * JSON loader.  It depends on the schema being stored with the data when
 * conceivably you could write a loader that determines the schema from the
 * JSON.  It is also not well tested, for functionality or performance.  It
 * works for simple demonstrations.
 *
 * Also note that this loader and the associated storage function require a
 * version of Pig that has PIG-2112 to work with complex data.
 */
public class JsonLoader extends LoadFunc implements LoadMetadata {

    protected RecordReader reader = null;
    protected ResourceFieldSchema[] fields = null;
    protected final Log log = LogFactory.getLog(getClass());

    private String udfcSignature = null;
    private JsonFactory jsonFactory = null;
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory bagFactory = BagFactory.getInstance();
    
    /**
     * Communicate to the loader the location of the object(s) being loaded.  
     * The location string passed to the LoadFunc here is the return value of 
     * {@link LoadFunc#relativeToAbsolutePath(String, Path)}. Implementations
     * should use this method to communicate the location (and any other
     * information) to its underlying InputFormat through the Job object.
     * 
     * This method will be called in the backend multiple times. Implementations
     * should bear in mind that this method is called multiple times and should
     * ensure there are no inconsistent side effects due to the multiple calls.
     * 
     * @param location Location as returned by 
     * {@link LoadFunc#relativeToAbsolutePath(String, Path)}
     * @param job the {@link Job} object
     * store or retrieve earlier stored information from the {@link UDFContext}
     * @throws IOException if the location is not valid.
     */
    public void setLocation(String location, Job job) throws IOException {
        // Tell our input format where we will be reading from
        FileInputFormat.setInputPaths(job, location);
    }
    
    /**
     * This will be called during planning on the front end. This is the
     * instance of InputFormat (rather than the class name) because the 
     * load function may need to instantiate the InputFormat in order 
     * to control how it is constructed.
     * @return the InputFormat associated with this loader.
     * @throws IOException if there is an exception during InputFormat 
     * construction
     */
    @SuppressWarnings("unchecked")
    public InputFormat getInputFormat() throws IOException {
        // We will use TextInputFormat, the default Hadoop input format for
        // text.  It has a LongWritable key that we will ignore, and the value
        // is a Text (a string writable) that the JSON data is in.
        return new TextInputFormat();
    }

    /**
     * This will be called on the front end during planning and not on the back 
     * end during execution.
     * @return the {@link LoadCaster} associated with this loader.
     * Returning null indicates that casts from byte array are not supported
     * for this loader. 
     * @throws IOException if there is an exception during LoadCaster 
     */
    public LoadCaster getLoadCaster() throws IOException {
        // We do not expect to do casting of byte arrays, because we will be
        // returning typed data.
        return null;
    }

    /**
     * Initializes LoadFunc for reading data.  This will be called during
     * execution before any calls to getNext.  The RecordReader needs to be
     * passed here because it has been instantiated for a particular InputSplit.
     * @param reader {@link RecordReader} to be used by this instance of
     * the LoadFunc
     * @param split The input {@link PigSplit} to process
     * @throws IOException if there is an exception during initialization
     */
    @SuppressWarnings("unchecked")
    public void prepareToRead(RecordReader reader, PigSplit split)
    throws IOException {
        this.reader = reader;
        
        // Get the schema string from the UDFContext object.
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p =
            udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        String strSchema = p.getProperty("pig.jsonloader.schema");
        if (strSchema == null) {
            throw new IOException("Could not find schema in UDF context");
        }

        // Parse the schema from the string stored in the properties object.
        ResourceSchema schema =
            new ResourceSchema(Utils.getSchemaFromString(strSchema));
        fields = schema.getFields();

        jsonFactory = new JsonFactory();
    }


    /**
     * Retrieves the next tuple to be processed. Implementations should NOT
     * reuse tuple objects (or inner member objects) they return across calls
     * and should return a different tuple object in each call.
     * @return the next tuple to be processed or null if there are no more
     * tuples to be processed.
     * @throws IOException if there is an exception while retrieving the next
     * tuple
     */
    public Tuple getNext() throws IOException {
        Text val = null;
        try {
            // Read the next key value pair from the record reader.  If it's
            // finished, return null
            if (!reader.nextKeyValue()) return null;

            // Get the current value.  We don't use the key.
            val = (Text)reader.getCurrentValue();
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }

        // Create a parser specific for this input line.  This may not be the
        // most efficient approach.
        ByteArrayInputStream bais = new ByteArrayInputStream(val.getBytes());
        JsonParser p = jsonFactory.createJsonParser(bais);

        // Create the tuple we will be returning.  We create it with the right
        // number of fields, as the Tuple object is optimized for this case.
        Tuple t = tupleFactory.newTuple(fields.length);

        // Read the start object marker.  Throughout this file if the parsing
        // isn't what we expect we return a tuple with null fields rather than
        // throwing an exception.  That way a few mangled lines don't fail the
        // job.
        if (p.nextToken() != JsonToken.START_OBJECT) {
            log.warn("Bad record, could not find start of record " +
                val.toString());
            return t;
        }

        // Read each field in the record
        for (int i = 0; i < fields.length; i++) {
            t.set(i, readField(p, fields[i], i));
        }

        if (p.nextToken() != JsonToken.END_OBJECT) {
            log.warn("Bad record, could not find end of record " +
                val.toString());
            return t;
        }
        p.close();
        return t;
    }

    private Object readField(JsonParser p,
                             ResourceFieldSchema field,
                             int fieldnum) throws IOException {
        // Read the next token
        JsonToken tok = p.nextToken();
        if (tok == null) {
            log.warn("Early termination of record, expected " + fields.length
                + " fields bug found " + fieldnum);
            return null;
        }

        // Check to see if this value was null
        if (tok == JsonToken.VALUE_NULL) return null;

        // Read based on our expected type
        switch (field.getType()) {
        case DataType.INTEGER:
            // Read the field name
            p.nextToken();
            return p.getValueAsInt();

        case DataType.LONG:
            p.nextToken();
            return p.getValueAsLong();

        case DataType.FLOAT:
            p.nextToken();
            return (float)p.getValueAsDouble();

        case DataType.DOUBLE:
            p.nextToken();
            return p.getValueAsDouble();

        case DataType.BYTEARRAY:
            p.nextToken();
            byte[] b = p.getBinaryValue();
            // Use the DBA constructor that copies the bytes so that we own
            // the memory
            return new DataByteArray(b, 0, b.length);

        case DataType.CHARARRAY:
            p.nextToken();
            return p.getText();

        case DataType.MAP:
            // Should be a start of the map object
            if (p.nextToken() != JsonToken.START_OBJECT) {
                log.warn("Bad map field, could not find start of object, field "
                    + fieldnum);
                return null;
            }
            Map<String, String> m = new HashMap<String, String>();
            while (p.nextToken() != JsonToken.END_OBJECT) {
                String k = p.getCurrentName();
                String v = p.getText();
                m.put(k, v);
            }
            return m;

        case DataType.TUPLE:
            if (p.nextToken() != JsonToken.START_OBJECT) {
                log.warn("Bad tuple field, could not find start of object, "
                    + "field " + fieldnum);
                return null;
            }

            ResourceSchema s = field.getSchema();
            ResourceFieldSchema[] fs = s.getFields();
            Tuple t = tupleFactory.newTuple(fs.length);

            for (int j = 0; j < fs.length; j++) {
                t.set(j, readField(p, fs[j], j));
            }

            if (p.nextToken() != JsonToken.END_OBJECT) {
                log.warn("Bad tuple field, could not find end of object, "
                    + "field " + fieldnum);
                return null;
            }
            return t;

        case DataType.BAG:
            if (p.nextToken() != JsonToken.START_ARRAY) {
                log.warn("Bad bag field, could not find start of array, "
                    + "field " + fieldnum);
                return null;
            }

            s = field.getSchema();
            fs = s.getFields();
            // Drill down the next level to the tuple's schema.
            s = fs[0].getSchema();
            fs = s.getFields();

            DataBag bag = bagFactory.newDefaultBag();

            JsonToken innerTok;
            while ((innerTok = p.nextToken()) != JsonToken.END_ARRAY) {
                if (innerTok != JsonToken.START_OBJECT) {
                    log.warn("Bad bag tuple field, could not find start of "
                        + "object, field " + fieldnum);
                    return null;
                }

                t = tupleFactory.newTuple(fs.length);
                for (int j = 0; j < fs.length; j++) {
                    t.set(j, readField(p, fs[j], j));
                }

                if (p.nextToken() != JsonToken.END_OBJECT) {
                    log.warn("Bad bag tuple field, could not find end of "
                        + "object, field " + fieldnum);
                    return null;
                }
                bag.add(t);
            }
            return bag;
        default:
            throw new IOException("Unknown type in input schema: " +
                field.getType());
        }

    }

    //------------------------------------------------------------------------
    
    /**
     * This method will be called by Pig both in the front end and back end to
     * pass a unique signature to the {@link LoadFunc}. The signature can be used
     * to store into the {@link UDFContext} any information which the 
     * {@link LoadFunc} needs to store between various method invocations in the
     * front end and back end. A use case is to store {@link RequiredFieldList} 
     * passed to it in {@link LoadPushDown#pushProjection(RequiredFieldList)} for
     * use in the back end before returning tuples in {@link LoadFunc#getNext()}.
     * This method will be call before other methods in {@link LoadFunc}
     * @param signature a unique signature to identify this LoadFunc
     */
    public void setUDFContextSignature(String signature) {
        udfcSignature = signature;
    }
       
    /**
     * Get a schema for the data to be loaded.  
     * @param location Location as returned by 
     * {@link LoadFunc#relativeToAbsolutePath(String, org.apache.hadoop.fs.Path)}
     * @param job The {@link Job} object - this should be used only to obtain 
     * cluster properties through {@link Job#getConfiguration()} and not to
     * set/query any runtime job information.  
     * @return schema for the data to be loaded. This schema should represent
     * all tuples of the returned data.  If the schema is unknown or it is
     * not possible to return a schema that represents all returned data,
     * then null should be returned. The schema should not be affected by
     * pushProjection, ie. getSchema should always return the original schema
     * even after pushProjection
     * @throws IOException if an exception occurs while determining the schema
     */
    public ResourceSchema getSchema(String location, Job job)
    throws IOException {
        // Open the schema file and read the schema
        // Get an HDFS handle.
        FileSystem fs = FileSystem.get(job.getConfiguration());
        DataInputStream in = fs.open(new Path(location + "/_schema"));
        String line = in.readLine();
        in.close();

        // Parse the schema
        ResourceSchema s =
            new ResourceSchema(Utils.getSchemaFromString(line));
        if (s == null) {
            throw new IOException("Unable to parse schema found in file " +
                location + "/_schema");
        }

        // Now that we have determined the schema, store it in our
        // UDFContext properties object so we have it when we need it on the
        // backend
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p =
            udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        p.setProperty("pig.jsonloader.schema", line);

        return s;
    }



    /**
     * Get statistics about the data to be loaded.  If no statistics are
     * available, then null should be returned.
     * @param location Location as returned by 
     * {@link LoadFunc#relativeToAbsolutePath(String, org.apache.hadoop.fs.Path)}
     * @param job The {@link Job} object - this should be used only to obtain 
     * cluster properties through {@link Job#getConfiguration()} and not to set/query
     * any runtime job information.  
     * @return statistics about the data to be loaded.  If no statistics are
     * available, then null should be returned.
     * @throws IOException if an exception occurs while retrieving statistics
     */
    public ResourceStatistics getStatistics(String location, Job job) 
    throws IOException {
        // We don't implement this one.
        return null;
    }

    /**
     * Find what columns are partition keys for this input.
     * @param location Location as returned by 
     * {@link LoadFunc#relativeToAbsolutePath(String, org.apache.hadoop.fs.Path)}
     * @param job The {@link Job} object - this should be used only to obtain 
     * cluster properties through {@link Job#getConfiguration()} and not to
     * set/query any runtime job information.  
     * @return array of field names of the partition keys. Implementations 
     * should return null to indicate that there are no partition keys
     * @throws IOException if an exception occurs while retrieving partition keys
     */
    public String[] getPartitionKeys(String location, Job job)
    throws IOException {
        // We don't have partitions
        return null;
    }

    /**
     * Set the filter for partitioning.  It is assumed that this filter
     * will only contain references to fields given as partition keys in
     * getPartitionKeys. So if the implementation returns null in 
     * {@link #getPartitionKeys(String, Job)}, then this method is not
     * called by Pig runtime. This method is also not called by the Pig runtime
     * if there are no partition filter conditions. 
     * @param partitionFilter that describes filter for partitioning
     * @throws IOException if the filter is not compatible with the storage
     * mechanism or contains non-partition fields.
     */
    public void setPartitionFilter(Expression partitionFilter)
    throws IOException {
        // We don't have partitions
    }
}
