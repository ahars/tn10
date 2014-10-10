import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.api.java.JavaEsSpark;

import java.util.Map;

public class SparkReadFromES {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("SparkFromES")
                .setMaster("local")
                .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getName())
                .set("es.resource", "sparky/LogWrite")
                .set("es.index.read.missing.as.empty", "yes");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc);

//        System.out.println(esRDD.first().toString());

        Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
        Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");

        JavaRDD<Map<String, ?>> javaRDD = sc.parallelize(ImmutableList.of(numbers, airports));
        JavaEsSpark.saveToEs(javaRDD, "spark/docs");

        System.out.println(JavaEsSpark.esRDD(sc).first().size());

//        sc.stop();
    }
}

