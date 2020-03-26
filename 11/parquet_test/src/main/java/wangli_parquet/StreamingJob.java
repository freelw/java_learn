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
//import org.apache.flink.core.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;


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
		if (args.length == 0) {
			System.out.println("please input action.");
			return ;
		}
		String action = args[0];
		String basePath = StreamingJob.class.getProtectionDomain().getCodeSource().getLocation().getPath() + "/../";
		String avscPath = basePath + "./test_schema.avsc";
		String parquetPath = basePath + "./test_record.parquet";
		Path path = new Path(parquetPath);
		Schema schema = new Schema.Parser().parse(new File(avscPath));
		if (action.equals("write")) {
			System.out.println(schema.toString());
			GenericRecord record = new GenericData.Record(schema);
			record.put("domain", "baidu.com");
			record.put("url", "http://www.baidu.com/");
			ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
				.withSchema(schema)
				.build();
			try {
				writer.write(record);
			} catch (IOException e) {
				e.printStackTrace();
			}
			writer.close();
		} else if (action.equals("read")) {
			ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path)
				.build();
			GenericRecord record;
			while ((record = reader.read())!= null){
				System.out.println(record);
			}
		} else if (action.equals("sink")) {
			String outputBasePath = basePath + "/sinkoutput/";
			SinkTask.work(schema, outputBasePath);
			/*Schema schema = new Schema.Parser().parse(new File(avscPath));
			final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			DataStream<String> text = env.socketTextStream("192.168.3.7", 9000, "\n");
			DataStream<GenericRecord> webSiteDataStream = text.map((str) -> {
				WebSite webSite = new WebSite();
				String[] parts = str.split("\\s");
				GenericRecord record = new GenericData.Record(schema);
				record.put("domain", parts[0]);
				record.put("url", parts[1]);
				return record;
			});
			//webSiteDataStream.addSink(new SinkToMySQL());
			String outputBasePath = basePath + "/sinkoutput/";
			final StreamingFileSink<GenericRecord> sink = StreamingFileSink
				.forBulkFormat(new Path(outputBasePath), ParquetAvroWriters.forGenericRecord(schema))
				.build();
			webSiteDataStream.addSink(sink);
			env.execute("website task.");*/
		}
	}
}
