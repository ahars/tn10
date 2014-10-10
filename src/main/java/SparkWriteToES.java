import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.spark.api.java.JavaEsSpark;
import scala.collection.immutable.Map;

public class SparkWriteToES {

    public static void main(String[] args) {

        final String PATH = "C:\\Users\\IPPON_2\\Desktop\\tn10\\sparky\\src\\data\\";
        String filename = PATH + "\\sample.log";

        SparkConf conf = new SparkConf()
                .setAppName("SparkToES")
                .setMaster("local")
                .set("es.nodes", "localhost:9200")
                .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getName())
                .set("es.index.auto.create", "true");
                //.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat")
                //.set(ConfigurationOptions.ES_RESOURCE_WRITE, "spark_test/log");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> javaRDD = sc.textFile(filename).map(x -> ApacheAccessLog.parseFromLogLine(x).toString());
        System.out.println(javaRDD.first().toString());


        JavaEsSpark.saveToEs(javaRDD, "spark/logs");




        sc.stop();
    }
}

