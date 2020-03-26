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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.CheckpointingMode;

public class SinkTask {
    public static void work(String outputBasePath) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        ExecutionConfig executionConfig = env.getConfig();
        executionConfig.disableGenericTypes();
        env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
        DataStream<String> text = env.socketTextStream("192.168.3.7", 9000, "\n");
        DataStream<WebSite> webSiteDataStream = text.map((str) -> {
            String[] parts = str.split("\\s");
            WebSite record = new WebSite();
            record.domain = parts[0];
            record.url = parts[1];
            return record;
        });
        
        final StreamingFileSink<WebSite> sink = StreamingFileSink
            .forBulkFormat(new Path(outputBasePath), ParquetAvroWriters.forReflectRecord(WebSite.class))
            .build();
            /*.withBucketAssigner(new BucketAssigner<GenericRecord, String>() {
                    @Override
                    public String getBucketId(GenericRecord o, Context context) {
                        return "nopartition";
                    }
                    @Override
                    public SimpleVersionedSerializer getSerializer() {
                        return SimpleVersionedStringSerializer.INSTANCE;
                    }
                })*/
            
        webSiteDataStream.addSink(sink);
        env.execute("website task.");
    }
}