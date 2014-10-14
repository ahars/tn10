import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkCassandraConnector {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("SparkToCassandra")
                .setMaster("local")
                .set("spark.cassandra.connection.host", "127.0.0.1")
                .set("spark.cassandra.auth.username", "cassandra")
                .set("spark.cassandra.auth.password", "cassandra");
        JavaSparkContext sc = new JavaSparkContext(conf);


        sc.stop();
    }
}
