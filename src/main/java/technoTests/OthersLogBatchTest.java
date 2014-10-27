package technoTests;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.CassandraJavaUtil;
import com.datastax.spark.connector.cql.CassandraConnector;
import formatLog.Log;
import formatLog.ParseFromCassandra;
import formatLog.ParseFromLogLine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.spark.api.java.JavaEsSpark.saveJsonToEs;

public class OthersLogBatchTest {

    public static void main(String[] args) {

        //final String PATH = "C:\\Users\\IPPON_2\\Desktop\\tn10\\sparky\\src\\data\\";
        //String filename = PATH + "\\sample.log";
        final String PATH = "/Users/ahars/sparky/src/data/";
        String filename = PATH + "/sample.log";

        SparkConf conf = new SparkConf()
                .setAppName("OthersLogBatchTest")
                .setMaster("local")
                .set("es.nodes", "localhost:9200")
                .set("es.index.auto.create", "true")
                .set("spark.cassandra.connection.host", "localhost");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println(sc.getConf().toDebugString());

        /* Init ElasticSearch */
//        Node node = nodeBuilder().clusterName("elasticsearch").node();
  //      Client client = node.client();

        /* Init Cassandra */
//        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
/*        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS access;");
            session.execute("CREATE KEYSPACE access " +
                    "WITH replication = {" +
                    "'class': 'SimpleStrategy'," +
                    "'replication_factor': 1" +
                    "};");
            session.execute("CREATE TABLE IF NOT EXISTS access.log (" +
                    "id TIMEUUID PRIMARY KEY," +
                    "ip_adress MAP<TEXT, TEXT>," +
                    "lnglat LIST<FLOAT>," +
                    "client_id TEXT," +
                    "user_id TEXT," +
                    "date_time MAP<TEXT, TEXT>," +
                    "method TEXT," +
                    "endPoint TEXT," +
                    "protocol_name TEXT," +
                    "protocol_version TEXT," +
                    "response_code INT," +
                    "content_size INT," +
                    "others TEXT" +
                    ");");
        }
*/
        /* Save into Cassandra from file */
//        CassandraJavaUtil.javaFunctions(sc.textFile(filename).map(x -> ParseFromLogLine.logParse(x)), Log.class)
  //              .saveToCassandra("access", "log");

        System.out.println(sc.textFile(filename).map(x -> ParseFromLogLine.logParse(x).toString()).first());
        System.out.println(sc.textFile(filename).map(x -> ParseFromLogLine.logParse(x).toJSON().string()).first());
/*
        CassandraJavaUtil.javaFunctions(sc)
                .cassandraTable("access", "log")
                .map(x -> ParseFromCassandra.logParse(x.toString()).toJSON())
                .foreach(x -> System.out.println(x.string()));
*/
        /* Save into ElasticSearch from Cassandra */
//        saveJsonToEs(CassandraJavaUtil.javaFunctions(sc).cassandraTable("access", "log")
  //              .map(x -> ParseFromCassandra.logParse(x.toString()).toJSON().string()), "sparky/Batch");

    //    sc.stop();
    }
}