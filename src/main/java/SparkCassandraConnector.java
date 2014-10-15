import com.datastax.driver.core.Session;
import com.datastax.spark.connector.CassandraJavaUtil;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.common.xcontent.XContentBuilder;

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
                    "ip TEXT," +
                    "countryCode TEXT," +
                    "countryName TEXT," +
                    "region TEXT," +
                    "regionName TEXT," +
                    "city TEXT," +
                    "postalCode TEXT," +
                    "lnglat LIST<FLOAT>," +
                    "latitude FLOAT," +
                    "longitude FLOAT," +
                    "metroCode INT," +
                    "areaCode INT," +
                    "timezone TEXT," +
                    "clientID TEXT," +
                    "userID TEXT," +
                    "dateTimeString TEXT," +
                    "timestamp TEXT," +
                    "day INT," +
                    "date INT," +
                    "month INT," +
                    "year INT," +
                    "hours INT," +
                    "minutes INT," +
                    "seconds INT," +
                    "timezoneOffset INT," +
                    "method TEXT," +
                    "endPoint TEXT," +
                    "protocolName TEXT," +
                    "protocolVersion TEXT," +
                    "responseCode INT," +
                    "contentSize INT," +
                    "link TEXT," +
                    "mozillaName TEXT," +
                    "mozillaVersion TEXT," +
                    "osType TEXT," +
                    "osName TEXT," +
                    "osVersion TEXT," +
                    "webkitType TEXT," +
                    "webkitVersion TEXT," +
                    "renduHtmlName TEXT," +
                    "renduHtmlType TEXT," +
                    "chromeName TEXT," +
                    "chromeVersion TEXT," +
                    "safariName TEXT," +
                    "safariVersion TEXT," +
                    "PRIMARY KEY (endPoint)" +
                    ");");
        }

        JavaRDD<XContentBuilder> javaRDD = sc.textFile(filename).map(x -> ApacheAccessLog.parseFromLogLine(x).toJSON());
        javaRDD.foreach();
        CassandraJavaUtil.javaFunctions(javaRDD, XContentBuilder.class).saveToCassandra("access", "log");

        JavaRDD<String> cassandraRowsRDD = CassandraJavaUtil.javaFunctions(sc)
                .cassandraTable("access", "log")
                .map(x -> x.toString());
        System.out.println("Data as CassandraRows: \n" + StringUtils.join(cassandraRowsRDD.toArray(), "\n"));

        sc.stop();
    }
}
