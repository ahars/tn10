package formatLog;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseFromLogLine {

    private static final Logger logger = Logger.getLogger("ParseFromLogLine");

    // 1:ip 2:client_id 3:user_id 4:date_time 5:method 6:req 7:protocol 8:respcode 9:size 10:link 11:mozilla
    // 12:os 13:webkit 14:rendh_html 15:chrome 16:safari
    private static final String APACHE_ACCESS_LOG_PATTERN =
            "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+) " +
                    "\"(\\S+)\" \"(\\S+) \\((\\S+\\s\\S+\\s\\S+\\s\\S+\\s\\S+\\s\\S+)\\) (\\S+) " +
                    "\\((\\S+, \\S+ \\S+)\\) (\\S+) (\\S+)\"$";
    private static final Pattern PATTERN = Pattern.compile(APACHE_ACCESS_LOG_PATTERN);

    public static ApacheAccessLog apacheAccessLogParse(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            logger.log(Level.ALL, "Cannot parse logline" + logline);
            throw new RuntimeException("Error parsing logline");
        }
        return new ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
                m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10),
                m.group(11), m.group(12), m.group(13), m.group(14), m.group(15), m.group(16));
    }


}
