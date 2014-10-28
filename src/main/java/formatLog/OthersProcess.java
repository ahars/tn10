package formatLog;

import java.util.HashMap;
import java.util.Map;

public class OthersProcess {

    static final String OTHERS_PATTERN_1 = "^\"(.*)";
    static final String OTHERS_PATTERN_2 = "(.*)\"$";
    static final String OTHERS_PATTERN_3 = "(.*)\"(.*)";
    static final String WEBPAGE_PATTERN = "http://";

    public static void main(String[] args) {

        String st = "\"http://manager.skytill.fr/index.php\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) " +
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36\"";

        Map<String, String> others = null;
        others = new HashMap<>();

        String[] splitting1 = st.split("\" \"");
        String[] splitting2 = null;
        String temp = null;

        for(int i = 0; i < splitting1.length; i++) {

            splitting1[i] = splitting1[i].replaceFirst("\"", "");
            if (splitting1[i].startsWith(WEBPAGE_PATTERN)) {
                others.put("webpage", splitting1[i]);
            } else {
                splitting2 = splitting1[i].split(" ");
                int j = 0;
                while(j < splitting2.length) {

                    if (splitting2[j].startsWith("(")) {
                        temp = "";
                        do {
                            temp = temp.concat(splitting2[j] + " ");
                            j++;
                        } while (!splitting2[j].endsWith(")"));
                        temp = temp.concat(splitting2[j]);
                        others.put("parenthese", temp);
                    } else if (splitting2[j].contains("/")) {

                        j++;
                    }




                }

            }



        }

        System.out.println("\n\n\n" + others);
    }
}
