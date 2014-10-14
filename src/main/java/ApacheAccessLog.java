
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.lang.String;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

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

    private String[] getProtocolToString() {
        Pattern pro = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher mpro = pro.matcher(protocol);

        if (!mpro.find()) {
            logger.log(Level.ALL, "Cannot parse protocol " + protocol);
            throw new RuntimeException("Error parsing protocol");
        }
        return new String[]{mpro.group(1), mpro.group(2)};
    }

    private String[] getWebkitToString() {
        Pattern wk = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher mwk = wk.matcher(webkit);

        if (!mwk.find()) {
            logger.log(Level.ALL, "Cannot parse webkit " + webkit);
            throw new RuntimeException("Error parsing webkit");
        }

        return new String[]{mwk.group(1), mwk.group(2)};
    }

    private String[] getRenduHTMLToString() {
        Pattern rendu = Pattern.compile("^(\\S+), (\\S+) (\\S+)");
        Matcher mrendu = rendu.matcher(renduHtml);

        if (!mrendu.find()) {
            logger.log(Level.ALL, "Cannot parse renduHtml " + renduHtml);
            throw new RuntimeException("Error parsing renduHtml");
        }

        return new String[]{mrendu.group(1), mrendu.group(3)};
    }

    private String[] getChromeVersionToString() {
        Pattern chr = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher mchr = chr.matcher(chromeVersion);

        if (!mchr.find()) {
            logger.log(Level.ALL, "Cannot parse chromeVersion " + chromeVersion);
            throw new RuntimeException("Error parsing chromeVersion");
        }

        return new String[]{mchr.group(1), mchr.group(2)};
    }

    private String[] getSafariVersionToString() {
        Pattern saf = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher msaf = saf.matcher(safariVersion);

        if (!msaf.find()) {
            logger.log(Level.ALL, "Cannot parse safariVersion " + safariVersion);
            throw new RuntimeException("Error parsing safariVersion");
        }

        return new String[]{msaf.group(1), msaf.group(2)};
    }

    private String[] getMozillaVersionToString() {
        Pattern moz = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher mmoz = moz.matcher(mozillaVersion);

        if (!mmoz.find()) {
            logger.log(Level.ALL, "Cannot parse mozillaVersion " + mozillaVersion);
            throw new RuntimeException("Error parsing mozillaVersion");
        }

        return new String[]{mmoz.group(1), mmoz.group(2)};
    }

    private String[] getOsToString() {
        Pattern pos = Pattern.compile("^(\\S+); (.*) (\\S+)");
        Matcher mos = pos.matcher(os);

        if (!mos.find()) {
            logger.log(Level.ALL, "Cannot parse os " + os);
            throw new RuntimeException("Error parsing os");
        }

        return new String[]{mos.group(1), mos.group(2), mos.group(3)};
    }

    private String[] getDateTimeToString() {

        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss ZZZ", Locale.US);
        Date date = null;

        try {
            date = formatter.parse(dateTimeString);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return new String[]{dateTimeString, String.valueOf(date.getTime()), String.valueOf(date.getDay()),
                String.valueOf(date.getDate()), String.valueOf(date.getMonth()),
                String.valueOf(date.getYear() + 1900), String.valueOf(date.getHours()),
                String.valueOf(date.getMinutes()), String.valueOf(date.getSeconds()),
                String.valueOf(date.getTimezoneOffset())};
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
                ip.locationIpToIndexString(), clientIdentd, userID, getDateTimeToString(),
                method, endpoint, getProtocolToString(), responseCode, contentSize, link,
                getMozillaVersionToString(), getOsToString(), getWebkitToString(),
                getRenduHTMLToString(), getChromeVersionToString(),
                getSafariVersionToString()
        );

    }

    public XContentBuilder toJSON() throws IOException {

        return jsonBuilder()
                .startObject()
                .field("ip", ip.getIp())
                .field("countryCode", ip.getCountryCode())
                .field("countryName", ip.getCountryName())
                .field("region", ip.getRegion())
                .field("regionName", ip.getRegName())
                .field("city", ip.getCity())
                .field("postalCode", ip.getPostalCode())
                .field("lnglat", ip.getLngLat())
                .field("latitude", ip.getLatitude())
                .field("longitude", ip.getLongitude())
                .field("metroCode", ip.getMetroCode())
                .field("areaCode", ip.getAreaCode())
                .field("timezone", ip.getTimezone())
                .field("clientID", clientIdentd)
                .field("userID", userID)
                .field("dateTimeString", getDateTimeToString()[0])
                .field("timestamp", getDateTimeToString()[1])
                .field("day", Integer.valueOf(getDateTimeToString()[2]))
                .field("date", Integer.valueOf(getDateTimeToString()[3]))
                .field("month", Integer.valueOf(getDateTimeToString()[4]))
                .field("year", Integer.valueOf(getDateTimeToString()[5]))
                .field("hours", Integer.valueOf(getDateTimeToString()[6]))
                .field("minutes", Integer.valueOf(getDateTimeToString()[7]))
                .field("seconds", Integer.valueOf(getDateTimeToString()[8]))
                .field("timezoneOffset", Integer.valueOf(getDateTimeToString()[9]))
                .field("method", method)
                .field("endPoint", endpoint)
                .field("protocolName", getProtocolToString()[0])
                .field("protocolVersion", getProtocolToString()[1])
                .field("responseCode", responseCode)
                .field("contentSize", contentSize)
                .field("link", link)
                .field("mozillaName", getMozillaVersionToString()[0])
                .field("mozillaVersion", getMozillaVersionToString()[1])
                .field("osType", getOsToString()[0])
                .field("osName", getOsToString()[1])
                .field("osVersion", getOsToString()[2])
                .field("kitType", getWebkitToString()[0])
                .field("kitVersion", getWebkitToString()[1])
                .field("renduHtmlName", getRenduHTMLToString()[0])
                .field("renduHtmlType", getRenduHTMLToString()[1])
                .field("chromeName", getChromeVersionToString()[0])
                .field("chromeVersion", getChromeVersionToString()[1])
                .field("safariName", getSafariVersionToString()[0])
                .field("safariVersion", getSafariVersionToString()[1])
                .endObject();
    }
}