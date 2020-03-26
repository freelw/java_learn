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

package wangli_parquet;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.File;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.hadoop.ParquetWriter;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		Schema schema = new Schema.Parser().parse(new File("/Volumes/data/liwang/project/java_learn/10/parquet_test/test_schema.avsc"));
		System.out.println(schema.toString());
		Path path = new Path("/Volumes/data/liwang/project/java_learn/10/parquet_test/test_record.parquet");
		GenericRecord record = new GenericData.Record(schema);
		record.put("left", "abc");
		record.put("right", "def");
		ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
			.withSchema(schema)
			.build();
		try {
			writer.write(record);
		} catch (IOException e) {
			e.printStackTrace();
		}
		writer.close();
	}
}
