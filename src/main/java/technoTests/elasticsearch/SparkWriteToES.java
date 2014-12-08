package technoTests.elasticsearch;
/*
import formatLog.ParseFromLogLine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.elasticsearch.spark.api.java.JavaEsSpark;

public class SparkWriteToES {

    public static void main(String[] args) {

        final String PATH = "C:\\Users\\IPPON_2\\Desktop\\tn10\\sparky\\src\\data\\";
        //final String PATH = "/Users/ahars/sparky/src/data/";
        String filename = PATH + "sample.log";

        SparkConf conf = new SparkConf()
                .setAppName("SparkToES")
                .setMaster("local")
                .set("es.nodes", "localhost:9200")
                .set("es.index.auto.create", "true");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println(sc.getConf().toDebugString());

        JavaEsSpark.saveJsonToEs(sc.textFile(filename).map(x -> ParseFromLogLine.logParse(x).toJSON().string()),
                "sparky/WriteToES");

        sc.stop();
    }
}

*/
