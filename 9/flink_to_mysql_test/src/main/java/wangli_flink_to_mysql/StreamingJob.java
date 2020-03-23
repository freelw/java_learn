
package wangli_flink_to_mysql;
import wangli_flink_to_mysql.WebSite;
import wangli_flink_to_mysql.SinkToMySQL;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> text = env.socketTextStream("192.168.3.7", 9000, "\n");
		DataStream<WebSite> webSiteDataStream = text.map((str) -> {
			WebSite webSite = new WebSite();
			String[] parts = str.split("\\s");
			webSite.name = parts[0];
			webSite.url = parts[1];
			return webSite;
		});
		webSiteDataStream.addSink(new SinkToMySQL());
		env.execute("website task.");
	}
}
