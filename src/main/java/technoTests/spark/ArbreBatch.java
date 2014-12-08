package technoTests.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class ArbreBatch {

    public static void main(String[] args) {

        final String PATH = "C:\\Users\\IPPON_2\\tn10\\sparky\\src\\data\\";
        //final String PATH = "/Users/ahars/sparky/src/data/";

        SparkConf conf = new SparkConf()
                .setAppName("SparkArbreBatch")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println(sc.getConf().toDebugString());

        String filename = PATH + "arbresalignementparis2010.csv";

        // TODO

        sc.stop();
    }
}
