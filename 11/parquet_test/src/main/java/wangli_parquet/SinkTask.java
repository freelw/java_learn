package wangli_parquet;
import org.apache.avro.Schema;
import org.apache.flink.core.fs.Path;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

public class SinkTask {
    public static void work(Schema schema, String outputBasePath) throws Exception {
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
        
        final StreamingFileSink<GenericRecord> sink = StreamingFileSink
            .forBulkFormat(new Path(outputBasePath), ParquetAvroWriters.forGenericRecord(schema))
            .build();
        webSiteDataStream.addSink(sink);
        env.execute("website task.");
    }
}