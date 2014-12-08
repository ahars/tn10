package technoTests.cassandra;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class SparkCassandraTest {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("SparkToCassandra")
                .setMaster("local")
                .set("spark.cassandra.connection.host", "localhost");
        JavaSparkContext sc = new JavaSparkContext(conf);

        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
        System.out.println(sc.getConf().toDebugString());

        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS test;");
            session.execute("CREATE KEYSPACE test " +
                    "WITH replication = {" +
                    "'class': 'SimpleStrategy'," +
                    "'replication_factor': 1" +
                    "};");
            session.execute("CREATE TABLE IF NOT EXISTS test.people (" +
                    "id INT," +
                    "name TEXT," +
                    "birth_date timestamp," +
                    "PRIMARY KEY (id)" +
                    ");");
            session.execute("CREATE INDEX ON test.people (name);");
            session.execute("INSERT INTO test.people (id, name, birth_date) " +
                    "VALUES (10, 'Catherine', '1987-12-02');");
            session.execute("INSERT INTO test.people (id, name, birth_date) " +
                    "VALUES (11, 'Isadora', '2004-09-08');");
            session.execute("INSERT INTO test.people (id, name, birth_date) " +
                    "VALUES (12, 'Anna', '1970-10-02');");
        }
        JavaRDD<String> cassandraRowsRDD = javaFunctions(sc).cassandraTable("test", "people").map(cassandraRow -> cassandraRow.toString());
        System.out.println("Data as CassandraRows: \n" + StringUtils.join("\n", cassandraRowsRDD.toArray()));

        System.out.println("Data as CassandraRows: \n" +
            StringUtils.join("\n", javaFunctions(sc)
                .cassandraTable("test", "people")
                .map(x -> x.toString())
                .toArray()));

        System.out.println("Data with only 'id' column fetched: \n" +
            StringUtils.join("\n", javaFunctions(sc)
                .cassandraTable("test", "people")
                .select("id")
                .map(x -> x.toString())
                .toArray()));

        System.out.println("Data filtered by the where clause (name='Anna'): \n" +
            StringUtils.join("\n", javaFunctions(sc)
                .cassandraTable("test", "people")
                .where("name=?", "Anna")
                .map(x -> x.toString())
                .toArray()));

        List<Person> people = Arrays.asList(
            new Person(1, "John", new Date()),
            new Person(2, "Troy", new Date()),
            new Person(3, "Andrew", new Date())
        );
        System.out.println(people.toString());

        JavaRDD<Person> rdd = sc.parallelize(people);
        System.out.println(rdd.first().toString());


        javaFunctions(rdd)..saveToCassandra("test", "people");

        System.out.println("Data as CassandraRows: \n" +
                StringUtils.join("\n", javaFunctions(sc)
                    .cassandraTable("test", "people")
                    .map(x -> x.toString())
                    .toArray()));

        CassandraJavaRDD<Person> rdd2 = javaFunctions(sc).cassandraTable("test", "people", Person.class);
        System.out.println(rdd2.first().getName() + " " + rdd2.first().getId() + " " + rdd2.first().getBirthDate());
*/
        sc.stop();
    }
}

