package formatLog;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface ParseFromCassandra {

    static final Logger logger = Logger.getLogger("ParseFromLogLine");

    static final String CASSANDRA_APACHE_ACCESS_LOG_PATTERN =
            "^CassandraRow\\{" +
                    "(\\S+): (\\S+), " +    // 2:id
                    "(\\S+): (\\S+), " +    // 4:area_code
                    "(\\S+): (\\S+), " +    // 6:chrome_name
                    "(\\S+): (\\S+), " +    // 8:chrome_version
                    "(\\S+): (\\S+), " +    // 10:city
                    "(\\S+): (\\S+), " +    // 12: client_id
                    "(\\S+): (\\S+), " +    // 14:content_size
                    "(\\S+): (\\S+), " +    // 16:country_code
                    "(\\S+): (\\S+), " +    // 18:country_name
                    "(\\S+): (\\S+), " +    // 20:date
                    "(\\S+): (\\d{2}\\/(\\S+)\\/\\d{4}:\\d{2}:\\d{2}:\\d{2}\\s[+\\-]\\d{4}), " +  //22:date_time_string
                    "(\\S+): (\\S+), " +    // 25:day
                    "(\\S+): (\\S+), " +    // 27:endpoint
                    "(\\S+): (\\S+), " +    // 29:hours
                    "(\\S+): (\\S+), " +    // 31:ip
                    "(\\S+): (\\S+), " +    // 33:latitude
                    "(\\S+): (\\S+), " +    // 35:link
                    "(\\S+): (\\S+), " +    // 37:lnglat
                    "(\\S+): (\\S+), " +    // 39:longitude
                    "(\\S+): (\\S+), " +    // 41:method
                    "(\\S+): (\\S+), " +    // 43:metro_code
                    "(\\S+): (\\S+), " +    // 45:minutes
                    "(\\S+): (\\S+), " +    // 47:month
                    "(\\S+): (\\S+), " +    // 49:mozilla_name
                    "(\\S+): (\\S+), " +    // 51:mozilla_version
                    "(\\S+): (.*), " +      // 53:os_name
                    "(\\S+): (\\S+), " +    // 55:os_type
                    "(\\S+): (\\S+), " +    // 57:os_version
                    "(\\S+): (\\S+), " +    // 59:postal_code
                    "(\\S+): (\\S+), " +    // 61:protocol_name
                    "(\\S+): (\\S+), " +    // 63:protocol_version
                    "(\\S+): (\\S+), " +    // 65:region_code
                    "(\\S+): (\\S+), " +    // 67:region_name
                    "(\\S+): (\\S+), " +    // 69:rendu_html_name
                    "(\\S+): (\\S+), " +    // 71:rendu_html_type
                    "(\\S+): (\\S+), " +    // 73:response_code
                    "(\\S+): (\\S+), " +    // 75:safari_name
                    "(\\S+): (\\S+), " +    // 77:safari_version
                    "(\\S+): (\\S+), " +    // 79:seconds
                    "(\\S+): (\\S+), " +    // 81:timestamp
                    "(\\S+): (\\S+), " +    // 83:timezone
                    "(\\S+): (\\S+), " +    // 85:timezone_offset
                    "(\\S+): (\\S+), " +    // 87:user_id
                    "(\\S+): (\\S+), " +    // 89:webkit_type
                    "(\\S+): (\\S+), " +    // 91:webkit_version
                    "(\\S+): (\\S+)" +      // 93:year
                    "\\}$";
    static final Pattern PATTERN = Pattern.compile(CASSANDRA_APACHE_ACCESS_LOG_PATTERN);

    public static ApacheAccessLog apacheAccessLogParse(String cassandraRow) {
        Matcher m = PATTERN.matcher(cassandraRow);
        if (!m.find()) {
            logger.log(Level.ALL, "Cannot parse cassandraRow" + cassandraRow);
            throw new RuntimeException("Error parsing cassandraRow");
        }
        return new ApacheAccessLog(m.group(2), m.group(4), m.group(6), m.group(8), m.group(10), m.group(12),
                m.group(14), m.group(16), m.group(18), m.group(20), m.group(22), m.group(25), m.group(27), m.group(29),
                m.group(31), m.group(33), m.group(35), m.group(37), m.group(39), m.group(41), m.group(43), m.group(45),
                m.group(47), m.group(49), m.group(51), m.group(53), m.group(55), m.group(57), m.group(59), m.group(61),
                m.group(63), m.group(65), m.group(67), m.group(69), m.group(71), m.group(73), m.group(75), m.group(77),
                m.group(79), m.group(81), m.group(83), m.group(85), m.group(87), m.group(89), m.group(91), m.group(93));
    }
}


