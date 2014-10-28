package technoTests;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.CassandraJavaUtil;
import com.datastax.spark.connector.CassandraRow;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.rdd.CassandraJavaRDD;
import com.datastax.spark.connector.rdd.CassandraRDD;
import com.datastax.spark.connector.types.TypeConverter;
import formatLog.Log;
import formatLog.ParseFromLogLine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class OthersLogBatchTest {

    public static void main(String[] args) {

        final String PATH = "C:\\Users\\IPPON_2\\Desktop\\tn10\\sparky\\src\\data\\";
        String filename = PATH + "\\sample.log";
        //final String PATH = "/Users/ahars/sparky/src/data/";
        //String filename = PATH + "/sample.log";

        SparkConf conf = new SparkConf()
                .setAppName("OthersLogBatchTest")
                .setMaster("local")
                .set("spark.cassandra.connection.host", "localhost");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println(sc.getConf().toDebugString());

        /* Init Cassandra */
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
        }

        System.out.println("SPARK = " + sc.textFile(filename).map(x -> ParseFromLogLine.logParse(x)).first());

        /* Save into Cassandra from file */
        CassandraJavaUtil.javaFunctions(sc.textFile(filename).map(x -> ParseFromLogLine.logParse(x)), Log.class)
                .saveToCassandra("access", "log");

        System.out.println(CassandraJavaUtil.javaFunctions(sc).cassandraTable("access", "log").first());
/*
        CassandraJavaRDD<Log> rdd = CassandraJavaUtil.javaFunctions(sc).cassandraTable("access", "log", Log.class);
        System.out.println(rdd.first().getIp());
*/
/*
        JavaRDD<String> rdd = CassandraJavaUtil.javaFunctions(sc).cassandraTable("access", "log", Log.class)
                .map(x -> x.toString());
        System.out.println(rdd.first());
*/
        CassandraJavaUtil.javaFunctions(sc).cassandraTable("access", "log").map(x -> x.getUUID("id"));
        CassandraJavaUtil.javaFunctions(sc).cassandraTable("access", "log").map(x -> x.getString("id"));
        CassandraJavaUtil.javaFunctions(sc).cassandraTable("access", "log").map(x -> x.getInt("id"));
        System.out.println(CassandraJavaUtil.javaFunctions(sc).cassandraTable("access", "log").map(x -> x.getMap("ip", new TypeConverter.StringConverter$(), new TypeConverter.StringConverter$())).first());

        //System.out.println("id = " + crow.map(x -> x.getUUID("id")).first().toString());
        //System.out.println("client_id = " + crow.map(x -> x.getString("client_id")).first().toString());
        //System.out.println("content_size = " + crow.map(x -> x.getInt("content_size")).first().toString());
        //System.out.println("ip = " + crow.map(x -> x..first().toString());


     //   JavaRDD jrdd = crow.map(x -> new Log(x));
       // System.out.println(jrdd.first().toString());

        //System.out.println(CassandraJavaUtil.javaFunctions(sc).cassandraTable("access", "log", Log.class).first());

        /* Save into ElasticSearch from Cassandra */
//        saveJsonToEs(CassandraJavaUtil.javaFunctions(sc).cassandraTable("access", "log")
  //              .map(x -> ParseFromCassandra.logParse(x.toString()).toJSON().string()), "sparky/Batch");

    //    sc.stop();
    }
}
