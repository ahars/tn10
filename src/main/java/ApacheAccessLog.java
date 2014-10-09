
import java.io.Serializable;
import java.lang.String;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
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

    private LocationIp ip;
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
    private String renduHtml;
    private String chromeVersion;
    private String safariVersion;

    private ApacheAccessLog(String ip, String clientIdentd, String userID, String dateTime,
                            String method, String endpoint, String protocol, String responseCode,
                            String contentSize, String link, String mozillaVersion, String os,
                            String webkit, String renduHtml, String chromeVersion, String safariVersion) {
        this.ip = new LocationIp(ip);
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
        this.renduHtml = renduHtml;
        this.chromeVersion = chromeVersion;
        this.safariVersion = safariVersion;
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

    public String getRenduHtml() {
        return renduHtml;
    }

    public String getChromeVersion() {
        return chromeVersion;
    }

    public String getSafariVersion() {
        return safariVersion;
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

    public void setRenduHtml(String renduHtml) {
        this.renduHtml = renduHtml;
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
            // 12:os 13:webkit 14:rendhHtml 15:chromeVersion 16:safariVersion
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

/*    @Override public String toString() {
        return String.format("%s %s %s [%s] \"%s %s %s\" %s %s \"%s\" \" %s %s %s %s %s %s\"",
                ip.getIp(), clientIdentd, userID, dateTimeString, method, endpoint,
                protocol, responseCode, contentSize, link, mozillaVersion, os, webkit,
                renduHtml, chromeVersion, safariVersion);
    }
*/
    private String getProtocolToIndexString() {
        Pattern pro = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher mpro = pro.matcher(protocol);

        if (!mpro.find()) {
            logger.log(Level.ALL, "Cannot parse protocol " + protocol);
            throw new RuntimeException("Error parsing protocol");
        }

        return String.format("{" +
                "\n\t\t\"nom\" : \"" + mpro.group(1) + "\"," +
                "\n\t\t\"version\" : \"" + mpro.group(2) + "\"" +
                "\n\t}");
    }

    private String getWebkitToIndexString() {
        Pattern wk = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher mwk = wk.matcher(webkit);

        if (!mwk.find()) {
            logger.log(Level.ALL, "Cannot parse webkit " + webkit);
            throw new RuntimeException("Error parsing webkit");
        }

        return String.format("{" +
                "\n\t\t\"type\" : \"" + mwk.group(1) + "\"," +
                "\n\t\t\"version\" : \"" + mwk.group(2) + "\"" +
                "\n\t}");
    }

    private String getRenduHTMLToIndexString() {
        Pattern rendu = Pattern.compile("^(\\S+), (\\S+) (\\S+)");
        Matcher mrendu = rendu.matcher(renduHtml);

        if (!mrendu.find()) {
            logger.log(Level.ALL, "Cannot parse renduHtml " + renduHtml);
            throw new RuntimeException("Error parsing renduHtml");
        }

        return String.format("{" +
                "\n\t\t\"nom\" : \"" + mrendu.group(1) + "\"," +
                "\n\t\t\"type\" : \"" + mrendu.group(3) + "\"" +
                "\n\t}");
    }

    private String getChromeVersionToIndexString() {
        Pattern chr = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher mchr = chr.matcher(chromeVersion);

        if (!mchr.find()) {
            logger.log(Level.ALL, "Cannot parse chromeVersion " + chromeVersion);
            throw new RuntimeException("Error parsing chromeVersion");
        }

        return String.format("{" +
                "\n\t\t\"nom\" : \"" + mchr.group(1) + "\"," +
                "\n\t\t\"version\" : \"" + mchr.group(2) + "\"" +
                "\n\t}");
    }

    private String getSafariVersionToIndexString() {
        Pattern saf = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher msaf = saf.matcher(safariVersion);

        if (!msaf.find()) {
            logger.log(Level.ALL, "Cannot parse safariVersion " + safariVersion);
            throw new RuntimeException("Error parsing safariVersion");
        }

        return String.format("{" +
                "\n\t\t\"nom\" : \"" + msaf.group(1) + "\"," +
                "\n\t\t\"version\" : \"" + msaf.group(2) + "\"" +
                "\n\t}");
    }

    private String getMozillaVersionToIndexString() {
        Pattern moz = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher mmoz = moz.matcher(mozillaVersion);

        if (!mmoz.find()) {
            logger.log(Level.ALL, "Cannot parse mozillaVersion " + mozillaVersion);
            throw new RuntimeException("Error parsing mozillaVersion");
        }

        return String.format("{" +
                "\n\t\t\"nom\" : \"" + mmoz.group(1) + "\"," +
                "\n\t\t\"version\" : \"" + mmoz.group(2) + "\"" +
                "\n\t}");
    }

    private String getOsToIndexString() {
        Pattern pos = Pattern.compile("^(\\S+); (.*) (\\S+)");
        Matcher mos = pos.matcher(os);

        if (!mos.find()) {
            logger.log(Level.ALL, "Cannot parse os " + os);
            throw new RuntimeException("Error parsing os");
        }

        return String.format("{" +
                "\n\t\t\"type\" : \"" + mos.group(1) + "\"," +
                "\n\t\t\"nom\" : \"" + mos.group(2) + "\"," +
                "\n\t\t\"version\" : \"" + mos.group(3) + "\"" +
                "\n\t}");
    }

    private String getDateTimeToIndexString() {

        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss ZZZ", Locale.US);
        Date date = null;

        try {
            date = formatter.parse(dateTimeString);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return String.format("{" +
                "\n\t\t\"dateTimeString\" : \"" + dateTimeString + "\"," +
                "\n\t\t\"timestamp\" : \"" + date.getTime() + "\"," +
                "\n\t\t\"day\" : \"" + date.getDay() + "\"," +
                "\n\t\t\"date\" : \"" + date.getDate() + "\"," +
                "\n\t\t\"month\" : \"" + date.getMonth() + "\"," +
                "\n\t\t\"year\" : \"" + (date.getYear() + 1900 ) + "\"," +
                "\n\t\t\"hours\" : \"" + date.getHours() + "\"," +
                "\n\t\t\"minutes\" : \"" + date.getMinutes() + "\"," +
                "\n\t\t\"seconds\" : \"" + date.getSeconds() + "\"," +
                "\n\t\t\"timezonOffset\" : \"" + date.getTimezoneOffset() + "\"" +
                "\n\t}");
    }

    @Override
    public String toString() {
        return String.format("{" +
                        "\n\t\"geoip\" : %s," +
                        "\n\t\"clientID\" : \"%s\"," +
                        "\n\t\"userID\" : \"%s\"," +
                        "\n\t\"date\" : %s," +
                        "\n\t\"method\" : \"%s\"," +
                        "\n\t\"endPoint\" : \"%s\"," +
                        "\n\t\"protocol\" : %s," +
                        "\n\t\"responseCode\" : \"%s\"," +
                        "\n\t\"contentSize\" : \"%s\"," +
                        "\n\t\"link\" : \"%s\"," +
                        "\n\t\"mozillaVersion\" : %s," +
                        "\n\t\"os\" : %s," +
                        "\n\t\"kit\" : %s," +
                        "\n\t\"renduHtml\" : %s," +
                        "\n\t\"chromeVersion\" : %s," +
                        "\n\t\"safariVersion\" : %s" +
                        "\n}",
                ip.locationIpToIndexString(), clientIdentd, userID, getDateTimeToIndexString(), method, endpoint,
                getProtocolToIndexString(), responseCode, contentSize, link,
                getMozillaVersionToIndexString(), getOsToIndexString(), getWebkitToIndexString(),
                getRenduHTMLToIndexString(), getChromeVersionToIndexString(),getSafariVersionToIndexString()
        );
    }

}