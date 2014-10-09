import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.spark.api.java.JavaEsSpark;
import org.elasticsearch.spark.rdd.JavaEsRDD;
import scala.collection.mutable.Map;

import java.util.Arrays;
import java.util.List;

public class LogWriteToES {

    public static void main(String[] args) {

        final String PATH = "C:\\Users\\IPPON_2\\Desktop\\tn10\\sparky\\src\\data\\";
        String filename = PATH + "\\sample.log";
        //String filename = PATH + "\\apache_logs_1.log";

        SparkConf conf = new SparkConf()
                .setAppName("SparkToES")
                .setMaster("local")
                .set("es.nodes", "localhost:9200")
                .set("spark.serializer", KryoSerializer.class.getName())
               ;// .set("es.index.auto.create", "true");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> javaRDD = sc.textFile(filename).map(x -> ApacheAccessLog.parseFromLogLine(x).toString());
        //JavaEsSpark.saveToEs(javaRDD, PATH);

        //JavaEsSpark.saveJsonToEs(javaRDD, "C:\\Users\\IPPON_2\\Google Drive\\tn10\\elasticsearch-1.3.4\\data\\Spark");

        sc.stop();
    }
}
