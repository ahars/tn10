import java.io.Serializable;
import java.lang.String;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class represents an Apache access log line.
 * See http://httpd.apache.org/docs/2.2/logs.html for more details.
 */
public class ApacheAccessLog implements Serializable {

    private static final Logger logger = Logger.getLogger("Access");

    private String ipAddress;
    private String clientIdentd;
    private String userID;
    private String dateTimeString;
    private String method;
    private String endpoint;
    private String protocol;
    private int responseCode;
    private long contentSize;
    private String link;
    private String mozillaVersion;
    private String os;
    private String webkit;
    private String khtml;
    private String chromeVersion;
    private String safariVersion;

    private ApacheAccessLog(String ipAddress, String clientIdentd, String userID, String dateTime,
                            String method, String endpoint, String protocol, String responseCode,
                            String contentSize, String link, String mozillaVersion, String os,
                            String webkit, String khtml, String chromeVersion, String safariVersion) {
        this.ipAddress = ipAddress;
        this.clientIdentd = clientIdentd;
        this.userID = userID;
        this.dateTimeString = dateTime;
        this.method = method;
        this.endpoint = endpoint;
        this.protocol = protocol;
        this.responseCode = Integer.parseInt(responseCode);
        this.contentSize = Long.parseLong(contentSize);
        this.link = link;
        this.mozillaVersion = mozillaVersion;
        this.os = os;
        this.webkit = webkit;
        this.khtml = khtml;
        this.chromeVersion = chromeVersion;
        this.safariVersion = safariVersion;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getClientIdentd() {
        return clientIdentd;
    }

    public String getUserID() {
        return userID;
    }

    public String getDateTimeString() {
        return dateTimeString;
    }

    public String getMethod() {
        return method;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getProtocol() {
        return protocol;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public long getContentSize() {
        return contentSize;
    }

    public String getLink() {
        return link;
    }

    public String getMozillaVersion() {
        return mozillaVersion;
    }

    public String getOs() {
        return os;
    }

    public String getWebkit() {
        return webkit;
    }

    public String getKhtml() {
        return khtml;
    }

    public String getChromeVersion() {
        return chromeVersion;
    }

    public String getSafariVersion() {
        return safariVersion;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public void setClientIdentd(String clientIdentd) {
        this.clientIdentd = clientIdentd;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public void setDateTimeString(String dateTimeString) {
        this.dateTimeString = dateTimeString;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public void setContentSize(long contentSize) {
        this.contentSize = contentSize;
    }

    public void setLink(String link){
        this.link = link;
    }

    public void setMozillaVersion(String mozillaVersion) {
        this.mozillaVersion = mozillaVersion;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public void setWebkit(String webkit) {
        this.webkit = webkit;
    }

    public void setKhtml(String khtml) {
        this.khtml = khtml;
    }

    public void setChromeVersion(String chromeVersion) {
        this.chromeVersion = chromeVersion;
    }

    public void setSafariVersion(String safariVersion) {
        this.safariVersion = safariVersion;
    }

    // Example Apache log line:
    //   127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
    private static final String LOG_ENTRY_PATTERN =
            // 1:IP 2:client 3:user 4:date_time 5:method 6:req 7:proto 8:respcode 9:size 10:link 11:mozillaVersion
            // 12:os 13:webkit 14:khtml 15:chromeVersion 16:safariVersion
            "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+) " +
                    "\"(\\S+)\" \"(\\S+) \\((\\S+\\s\\S+\\s\\S+\\s\\S+\\s\\S+\\s\\S+)\\) (\\S+) " +
                    "\\((\\S+, \\S+ \\S+)\\) (\\S+) (\\S+)\"";

    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    public static ApacheAccessLog parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            logger.log(Level.ALL, "Cannot parse logline" + logline);
            throw new RuntimeException("Error parsing logline");
        }

        return new ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
                m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10),
                m.group(11), m.group(12), m.group(13), m.group(14), m.group(15), m.group(16));
    }

    @Override public String toString() {
        return String.format("%s %s %s [%s] \"%s %s %s\" %s %s \"%s\" \"%s %s\"",
                ipAddress, clientIdentd, userID, dateTimeString, method, endpoint,
                protocol, responseCode, contentSize, link, os, webkit);
    }
}