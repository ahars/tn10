package formatLog;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Log implements Serializable {

    private static final Logger logger = Logger.getLogger("Log");

    private String clientId;
    private int contentSize;
    private Date dateTime;
    private String endpoint;
    private LocationIp ip;
    private String method;
    private String others;
    private String protocolName;
    private String protocolVersion;
    private int responseCode;
    private String userId;

    public Log(String ipAdress, String clientId, String userId, String dateString, String method, String endpoint,
               String protocol, String responseCode, String contentSize, String others) {

        this.ip = new LocationIp(ipAdress);
        this.clientId = clientId;
        this.userId = userId;
        this.dateTime = DateTimeFromString(dateString);
        this.method = method;
        this.endpoint = endpoint;
        this.protocolName = getProtocolToString(protocol)[0];
        this.protocolVersion = getProtocolToString(protocol)[1];
        this.responseCode = Integer.parseInt(responseCode);
        this.contentSize =  Integer.parseInt(contentSize);
        this.others = others;
    }

    private Date DateTimeFromString(String dateString) {
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss ZZZ", Locale.US);
        Date date = null;

        try {
            date = formatter.parse(dateString);
        } catch (ParseException e) {
            logger.warning("error parsing date");
        }
        return date;
    }

    private String[] getProtocolToString(String protocol) {
        Pattern pro = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher mpro = pro.matcher(protocol);

        if (!mpro.find()) {
            logger.log(Level.ALL, "Cannot parse protocol " + protocol);
            throw new RuntimeException("Error parsing protocol");
        }
        return new String[]{mpro.group(1), mpro.group(2)};
    }

    @Override
    public String toString() {
        return "Log{" +
                "clientId='" + clientId + '\'' +
                ", contentSize=" + contentSize +
                ", dateTime=" + dateTime.toString() +
                ", endpoint='" + endpoint + '\'' +
                ", ip=" + ip.toString() +
                ", method='" + method + '\'' +
                ", others='" + others + '\'' +
                ", protocolName='" + protocolName + '\'' +
                ", protocolVersion='" + protocolVersion + '\'' +
                ", responseCode=" + responseCode +
                ", userId='" + userId + '\'' +
                '}';
    }
}
