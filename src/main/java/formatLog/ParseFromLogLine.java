package formatLog;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface ParseFromLogLine {

    static final Logger logger = Logger.getLogger("ParseFromLogLine");

    static final String APACHE_ACCESS_LOG_PATTERN = "^(\\S+) " +    // 1:ip
            "(\\S+) " +     // 2:client_id
            "(\\S+) " +     // 3:user_id
            "\\[([\\w:/]+\\s[+\\-]\\d{4})\\] " +    // 4:date_time
            "\"(\\S+) " +   // 5:method
            "(\\S+) " +     // 6:req
            "(\\S+)\" " +   // 7:protocol
            "(\\d{3}) " +   // 8:respcode
            "(\\d+) " +     // 9:size
            "\"(\\S+)\" " + // 10:link
            "\"(\\S+) " +   // 11:mozilla
            "\\((\\S+\\s\\S+\\s\\S+\\s\\S+\\s\\S+\\s\\S+)\\) " +    // 12:os
            "(\\S+) " +     // 13:webkit
            "\\((\\S+, \\S+ \\S+)\\) " +    // 14:rendu_html
            "(\\S+) " +     // 15:chrome
            "(\\S+)\"$";    // 16:safari
    static final Pattern PATTERN = Pattern.compile(APACHE_ACCESS_LOG_PATTERN);

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
