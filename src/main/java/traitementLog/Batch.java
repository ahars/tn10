package traitementLog;
/*
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.CassandraJavaUtil;
import com.datastax.spark.connector.cql.CassandraConnector;
import formatLog.Log;
import formatLog.ParseFromLogLine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.spark.api.java.JavaEsSpark.saveJsonToEs;

public class Batch {

    public static void main(String[] args) {

        final String PATH = "C:\\Users\\IPPON_2\\Desktop\\tn10\\sparky\\src\\data\\";
        //final String PATH = "/Users/ahars/sparky/src/data/";
        //String filename = PATH + "sample.log";
        String filename = PATH + "apache_logs_1.log";

        SparkConf conf = new SparkConf()
                .setAppName("SparkBatch")
                .setMaster("local")
                .set("es.nodes", "localhost:9200")
                .set("es.index.auto.create", "true")
                .set("spark.cassandra.connection.host", "localhost");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println(sc.getConf().toDebugString());

        // Init ElasticSearch
        Node node = nodeBuilder().clusterName("elasticsearch").node();
        Client client = node.client();

        // Init Cassandra
        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS access;");
            session.execute("CREATE KEYSPACE access " +
                    "WITH replication = {" +
                    "'class': 'SimpleStrategy'," +
                    "'replication_factor': 1" +
                    "};");
            session.execute("CREATE TABLE IF NOT EXISTS access.log (" +
                    "id TIMEUUID PRIMARY KEY," +
                    "client_id TEXT," +
                    "content_size INT," +
                    "date_time MAP<TEXT, TEXT>," +
                    "endPoint TEXT," +
                    "ip MAP<TEXT, TEXT>," +
                    "lnglat LIST<FLOAT>," +
                    "method TEXT," +
                    "others TEXT," +
                    "protocol_name TEXT," +
                    "protocol_version TEXT," +
                    "response_code INT," +
                    "user_id TEXT" +
                    ");");
            session.execute("CREATE INDEX ON access.log (client_id);");
        }

        // Save into Cassandra from file
        CassandraJavaUtil.javaFunctions(sc.textFile(filename).map(x -> ParseFromLogLine.logParse(x)), Log.class)
                .saveToCassandra("access", "log");

        // Save into ElasticSearch from Cassandra
 //       saveJsonToEs(CassandraJavaUtil.javaFunctions(sc).cassandraTable("access", "log")
   //             .map(x -> new Log(x).toJSON().string()), "sparky/traitementLog.Batch");

        sc.stop();
    }
}
*/
