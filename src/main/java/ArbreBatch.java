import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class ArbreBatch {

    public static void main(String[] args) {

        final String PATH = "C:\\Users\\IPPON_2\\tn10\\sparky\\src\\data\\";

        SparkConf conf = new SparkConf()
                .setAppName("SparkArbreBatch")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filename = PATH + "\\arbresalignementparis2010.csv";
    }
}
