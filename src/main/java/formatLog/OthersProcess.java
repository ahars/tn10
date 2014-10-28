package formatLog;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OthersProcess {

    static final String OTHERS_PATTERN_1 = "^\"(.*)";
    static final String OTHERS_PATTERN_2 = "(.*)\"$";
    static final String OTHERS_PATTERN_3 = "(.*)\"(.*)";
    static final String OTHERS_PATTERN_4 = "http://(.*)";

    static final Pattern PATTERN4 = Pattern.compile(OTHERS_PATTERN_4);

    public static void main(String[] args) {

        String others = "\"http://manager.skytill.fr/index.php\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36\"";
        System.out.println("others = " + others);

        System.out.println("pattern 1 = " + others.matches(OTHERS_PATTERN_1));
        System.out.println("pattern 2 = " + others.matches(OTHERS_PATTERN_2));
        System.out.println("pattern 3 = " + others.matches(OTHERS_PATTERN_3));


        String[] splitting = others.split("\" ");
        String result = null;

        for(int i = 0; i < splitting.length; i++) {
            result = splitting[i].replaceFirst(OTHERS_PATTERN_4, "");

            Matcher m = PATTERN4.matcher(splitting[i]);

            if (!m.find()) {

            }
            System.out.println(splitting[i]);
            System.out.println(result);
        }
    }
}
