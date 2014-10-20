import com.datastax.driver.core.Session;
import com.datastax.spark.connector.CassandraJavaUtil;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.List;

public class SparkCassandraConnector {

    public static void main(String[] args) {

        final String PATH = "C:\\Users\\IPPON_2\\Desktop\\tn10\\sparky\\src\\data\\";
        String filename = PATH + "\\sample.log";

        SparkConf conf = new SparkConf()
                .setAppName("SparkToCassandra")
                .setMaster("local")
                .set("spark.cassandra.connection.host", "localhost");
        JavaSparkContext sc = new JavaSparkContext(conf);

        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
        System.out.println(sc.getConf().toDebugString());

        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS access;");
            session.execute("CREATE KEYSPACE access " +
                    "WITH replication = {" +
                    "'class': 'SimpleStrategy'," +
                    "'replication_factor': 1" +
                    "};");
            session.execute("CREATE TABLE IF NOT EXISTS access.log (" +
                    "id INT PRIMARY KEY," +
                    "ip TEXT," +
                    "country_code TEXT," +
                    "country_name TEXT," +
                    "region_code TEXT," +
                    "region_name TEXT," +
                    "city TEXT," +
                    "postal_code TEXT," +
                    "lnglat LIST<FLOAT>," +
                    "latitude FLOAT," +
                    "longitude FLOAT," +
                    "metro_code INT," +
                    "area_code INT," +
                    "timezone TEXT," +
                    "client_id TEXT," +
                    "user_id TEXT," +
                    "date_time_string TEXT," +
                    "timestamp TEXT," +
                    "day INT," +
                    "date INT," +
                    "month INT," +
                    "year INT," +
                    "hours INT," +
                    "minutes INT," +
                    "seconds INT," +
                    "timezone_offset INT," +
                    "method TEXT," +
                    "endPoint TEXT," +
                    "protocol_name TEXT," +
                    "protocol_version TEXT," +
                    "response_code INT," +
                    "content_size INT," +
                    "link TEXT," +
                    "mozilla_name TEXT," +
                    "mozilla_version TEXT," +
                    "os_type TEXT," +
                    "os_name TEXT," +
                    "os_version TEXT," +
                    "webkit_type TEXT," +
                    "webkit_version TEXT," +
                    "rendu_html_name TEXT," +
                    "rendu_html_type TEXT," +
                    "chrome_name TEXT," +
                    "chrome_version TEXT," +
                    "safari_name TEXT," +
                    "safari_version TEXT" +
                    ");");
        }

        List<ApacheAccessLog> list = sc.textFile(filename).map(x -> ApacheAccessLog.parseFromLogLine(x)).collect();
        JavaRDD<ApacheAccessLog> rdd = sc.parallelize(list);

        System.out.println(list.toString());
        System.out.println(rdd.first().toString());

        CassandraJavaUtil.javaFunctions(rdd, ApacheAccessLog.class).saveToCassandra("access", "log");

        JavaRDD<String> cassandraRowsRDD = CassandraJavaUtil.javaFunctions(sc)
                .cassandraTable("access", "log")
                .map(x -> x.toString());
        System.out.println("Data as CassandraRows: \n" + StringUtils.join(cassandraRowsRDD.toArray(), "\n"));

        sc.stop();
    }
}
