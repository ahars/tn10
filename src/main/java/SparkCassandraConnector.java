import com.datastax.driver.core.Session;
import com.datastax.spark.connector.CassandraJavaUtil;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkCassandraConnector {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("SparkToCassandra")
                .setMaster("local")
                .set("spark.cassandra.connection.host", "localhost");
        JavaSparkContext sc = new JavaSparkContext(conf);

        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
        System.out.println(sc.getConf().toDebugString());

        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS sparky_test;");
            session.execute("CREATE KEYSPACE sparky_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
            session.execute("CREATE TABLE sparky_test.products (id INT PRIMARY KEY, name TEXT, parents LIST<INT>);");
            session.execute("CREATE TABLE sparky_test.sales (id UUID PRIMARY KEY, product INT, price DECIMAL);");
            session.execute("CREATE TABLE sparky_test.summaries (product INT PRIMARY KEY, summary DECIMAL);");
        }

        JavaRDD<String> cassandraRowsRDD = CassandraJavaUtil.javaFunctions(sc)
                .cassandraTable("access", "test")
                .map(x -> x.toString());
        System.out.println("Data as CassandraRows: \n" + StringUtils.join(cassandraRowsRDD.toArray(), "\n"));

        sc.stop();
    }
}
