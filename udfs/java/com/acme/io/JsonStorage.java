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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreMetadata;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

/**
 * A JSON Pig store function.  Each Pig tuple is stored on one line (as one
 * value for TextOutputFormat) so that it can be read easily using
 * TextInputFormat.  Pig tuples are mapped to JSON objects.  Pig bags are
 * mapped to JSON arrays.  Pig maps are also mapped to JSON objects.  Maps are
 * assumed to be string to string.  A schema is stored in a side file to deal
 * with mapping between JSON and Pig types.  This class is not well tested for
 * functionality or performance.
 *
 * Also note that this store function and the associated loader require a
 * version of Pig that has PIG-2112 to work with complex data.
 */
public class JsonStorage extends StoreFunc implements StoreMetadata {

    protected RecordWriter writer = null;
    protected ResourceFieldSchema[] fields = null;

    private String udfcSignature = null;
    private JsonFactory jsonFactory = null;

    // Default size for the byte buffer, should fit most tuples.
    private static final int BUF_SIZE = 4 * 1024; 

    /*
     * Methods called on the front end
     */

    /**
     * Return the OutputFormat associated with StoreFunc.  This will be called
     * on the front end during planning and on the backend during
     * execution. 
     * @return the {@link OutputFormat} associated with StoreFunc
     * @throws IOException if an exception occurs while constructing the 
     * OutputFormat
     *
     */
    @Override
    public OutputFormat getOutputFormat() throws IOException {
        // We will use TextOutputFormat, the default Hadoop output format for
        // text.  The key is unused and the value will be a
        // Text (a string writable type) that we store our JSON data in.
        return new TextOutputFormat<LongWritable, Text>();
    }

    /**
     * Communicate to the storer the location where the data needs to be
     * stored.  The location string passed to the {@link StoreFunc} here is the 
     * return value of StoreFunc#relToAbsPathForStoreLocation(String, Path) 
     * This method will be called in the frontend and backend multiple times.
     * Implementations should bear in mind that this method is called multiple
     * times and should ensure there are no inconsistent side effects due to
     * the multiple calls.  checkSchema(ResourceSchema) will be called before
     * any call to setStoreLocation(String, Job).
     * 
     * @param location Location returned by 
     * relToAbsPathForStoreLocation(String, Path)
     * @param job The Job object
     * @throws IOException if the location is not valid.
     */

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        // FileOutputFormat has a utility method for setting up the output
        // location.  
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    /**
     * This method will be called by Pig both in the front end and back end to
     * pass a unique signature to the {@link StoreFunc} which it can use to store
     * information in the {@link UDFContext} which it needs to store between
     * various method invocations in the front end and back end. This method 
     * will be called before other methods in {@link StoreFunc}.  This is necessary
     * because in a Pig Latin script with multiple stores, the different
     * instances of store functions need to be able to find their (and only their)
     * data in the UDFContext object.  The default implementation is a no-op.
     * @param signature a unique signature to identify this StoreFunc
     */
    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        // store the signature so we can use it later
        udfcSignature = signature;
    }
 
    /**
     * Set the schema for data to be stored.  This will be called on the
     * front end during planning if the store is associated with a schema.
     * A Store function should implement this function to
     * check that a given schema is acceptable to it.  For example, it
     * can check that the correct partition keys are included;
     * a storage function to be written directly to an OutputFormat can
     * make sure the schema will translate in a well defined way.  Default implementation
     * is a no-op.
     * @param s to be checked
     * @throws IOException if this schema is not acceptable.  It should include
     * a detailed error message indicating what is wrong with the schema.
     */
    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        // We won't really check the schema here, we'll store it in our
        // UDFContext properties object so we have it when we need it on the
        // backend
        
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p =
            udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        p.setProperty("pig.jsonstorage.schema", s.toString());
    }


    /*
     * Methods called on the back end
     */

    /**
     * Initialize StoreFunc to write data.  This will be called during
     * execution on the backend before the call to putNext.
     * @param writer RecordWriter to use.
     * @throws IOException if an exception occurs during initialization
     */
    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        // Store the record writer reference so we can use it when it's time
        // to write tuples
        this.writer = writer;

        // Get the schema string from the UDFContext object.
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p =
            udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        String strSchema = p.getProperty("pig.jsonstorage.schema");
        if (strSchema == null) {
            throw new IOException("Could not find schema in UDF context");
        }

        // Parse the schema from the string stored in the properties object.
        ResourceSchema schema =
            new ResourceSchema(Utils.getSchemaFromString(strSchema));
        fields = schema.getFields();

        // Build a Json factory
        jsonFactory = new JsonFactory();
        jsonFactory.configure(
            JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, false);
    }

    /**
     * Write a tuple to the data store.
     * @param t the tuple to store.
     * @throws IOException if an exception occurs during the write
     */
    public void putNext(Tuple t) throws IOException {
        // Build a ByteArrayOutputStream to write the JSON into
        ByteArrayOutputStream baos = new ByteArrayOutputStream(BUF_SIZE);
        // Build the generator
        JsonGenerator json =
            jsonFactory.createJsonGenerator(baos, JsonEncoding.UTF8);

        // Write the beginning of the top level tuple object
        json.writeStartObject();
        for (int i = 0; i < fields.length; i++) {
            writeField(json, fields[i], t.get(i));
        }
        json.writeEndObject();
        json.close();

        // Hand a null key and our string to Hadoop
        try {
            writer.write(null, new Text(baos.toByteArray()));
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }
    }

    private void writeField(JsonGenerator json,
                            ResourceFieldSchema field, 
                            Object d) throws IOException {

        // If the field is missing or the value is null, write a null
        if (d == null) {
            json.writeNullField(field.getName());
            return;
        }

        // Based on the field's type, write it out
        switch (field.getType()) {
        case DataType.INTEGER:
            json.writeNumberField(field.getName(), (Integer)d);
            return;

        case DataType.LONG:
            json.writeNumberField(field.getName(), (Long)d);
            return;

        case DataType.FLOAT:
            json.writeNumberField(field.getName(), (Float)d);
            return;

        case DataType.DOUBLE:
            json.writeNumberField(field.getName(), (Double)d);
            return;

        case DataType.BYTEARRAY:
            json.writeBinaryField(field.getName(), ((DataByteArray)d).get());
            return;

        case DataType.CHARARRAY:
            json.writeStringField(field.getName(), (String)d);
            return;

        case DataType.MAP:
            json.writeFieldName(field.getName());
            json.writeStartObject();
            for (Map.Entry<String, Object> e : ((Map<String, Object>)d).entrySet()) {
                json.writeStringField(e.getKey(), e.getValue().toString());
            }
            json.writeEndObject();
            return;

        case DataType.TUPLE:
            json.writeFieldName(field.getName());
            json.writeStartObject();

            ResourceSchema s = field.getSchema();
            if (s == null) {
                throw new IOException("Schemas must be fully specified to use "
                    + "this storage function.  No schema found for field " +
                    field.getName());
            }
            ResourceFieldSchema[] fs = s.getFields();

            for (int j = 0; j < fs.length; j++) {
                writeField(json, fs[j], ((Tuple)d).get(j));
            }
            json.writeEndObject();
            return;

        case DataType.BAG:
            json.writeFieldName(field.getName());
            json.writeStartArray();
            s = field.getSchema();
            if (s == null) {
                throw new IOException("Schemas must be fully specified to use "
                    + "this storage function.  No schema found for field " +
                    field.getName());
            }
            fs = s.getFields();
            if (fs.length != 1 || fs[0].getType() != DataType.TUPLE) {
                throw new IOException("Found a bag without a tuple "
                    + "inside!");
            }
            // Drill down the next level to the tuple's schema.
            s = fs[0].getSchema();
            if (s == null) {
                throw new IOException("Schemas must be fully specified to use "
                    + "this storage function.  No schema found for field " +
                    field.getName());
            }
            fs = s.getFields();
            for (Tuple t : (DataBag)d) {
                json.writeStartObject();
                for (int j = 0; j < fs.length; j++) {
                    writeField(json, fs[j], t.get(j));
                }
                json.writeEndObject();
            }
            json.writeEndArray();
            return;
        }
    }

    /**
     * Store statistics about the data being written.
     * @param stats statistics to be recorded
     * @param location Location as returned by 
     * {@link LoadFunc#relativeToAbsolutePath(String, org.apache.hadoop.fs.Path)}
     * @param job The {@link Job} object - this should be used only to obtain 
     * cluster properties through {@link Job#getConfiguration()} and not to
     * set/query any runtime job information.  
     * @throws IOException 
     */
    public void storeStatistics(ResourceStatistics stats,
                                String location,
                                Job job) throws IOException {
        // We don't implement this method
    }

    /**
     * Store schema of the data being written
     * @param schema Schema to be recorded
     * @param location Location as returned by 
     * {@link LoadFunc#relativeToAbsolutePath(String, org.apache.hadoop.fs.Path)}
     * @param job The {@link Job} object - this should be used only to obtain 
     * cluster properties through {@link Job#getConfiguration()} and not to
     * set/query any runtime job information.  
     * @throws IOException 
     */
    public void storeSchema(ResourceSchema schema, String location, Job job)
    throws IOException {
        // Store the schema in a side file in the same directory.  MapReduce
        // does not include files starting with "_" when reading data for a job.
        FileSystem fs = FileSystem.get(job.getConfiguration());
        DataOutputStream out = fs.create(new Path(location + "/_schema"));
        out.writeBytes(schema.toString());
        out.writeByte('\n');
        out.close();
    }

}
