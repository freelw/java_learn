package wangli_flink_to_mysql;
import java.sql.*;
import wangli_flink_to_mysql.WebSite;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;
public class SinkToMySQL extends RichSinkFunction<WebSite> {
    private PreparedStatement ps;
    private Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into websites(name, url) values(?, ?);";
        ps = this.connection.prepareStatement(sql);
    }
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
    @Override
    public void invoke(WebSite value, Context context) throws Exception {
        ps.setString(1, value.name);
        ps.setString(2, value.url);
        ps.executeUpdate();
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            System.out.println("连接数据库...");
            con = DriverManager.getConnection("jdbc:mysql://192.168.3.7:3306/RUNOOB?useSSL=false&serverTimezone=UTC", "root", "123");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }
}