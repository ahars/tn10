
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.lang.String;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ApacheAccessLog implements Serializable {

    private static final Logger logger = Logger.getLogger("Access");
    private static int id = 0;

    private String ip = null;
    private String countryCode = null;
    private String countryName = null;
    private String regionCode = null;
    private String regionName = null;
    private String city = null;
    private String postalCode = null;
    private List<Float> lnglat = null;
    private Float latitude = null;
    private Float longitude = null;
    private int metroCode = 0;
    private int areaCode = 0;
    private String timezone = null;
    private String clientID = null;
    private String userID = null;
    private String dateTimeString = null;
    private String timestamp = null;
    private int day = 0;
    private int date = 0;
    private int month = 0;
    private int year = 0;
    private int hours = 0;
    private int minutes = 0;
    private int seconds = 0;
    private int timezoneOffset = 0;
    private String method = null;
    private String endpoint = null;
    private String protocolName = null;
    private String protocolVersion = null;
    private int responseCode = 0;
    private long contentSize = 0;
    private String link = null;
    private String mozillaName = null;
    private String mozillaVersion = null;
    private String osType = null;
    private String osName = null;
    private String osVersion = null;
    private String webkitType = null;
    private String webkitVersion = null;
    private String renduHtmlName = null;
    private String renduHtmlType = null;
    private String chromeName = null;
    private String chromeVersion = null;
    private String safariName = null;
    private String safariVersion = null;

    public ApacheAccessLog() {}

    public ApacheAccessLog(String ip, String clientID, String userID, String dateString,
                            String method, String endpoint, String protocol, String responseCode,
                            String contentSize, String link, String mozilla, String os,
                            String webkit, String renduHtml, String chrome, String safari) {

        LocationIp location = new LocationIp(ip);

        this.id = getid();
        this.ip = location.getIp();
        this.countryCode = location.getCountryCode();
        this.countryName = location.getCountryName();
        this.regionCode = location.getRegionCode();
        this.regionName = location.getRegionName();
        this.city = location.getCity();
        this.postalCode = location.getPostalCode();
        this.lnglat = location.getLnglat();
        this.latitude = location.getLatitude();
        this.longitude = location.getLongitude();
        this.metroCode = location.getMetroCode();
        this.areaCode = location.getAreaCode();
        this.timezone = location.getTimezone();
        this.clientID = clientID;
        this.userID = userID;
        this.dateTimeString = getDateTimeToString(dateString)[0];
        this.timestamp = getDateTimeToString(dateString)[1];
        this.day = Integer.valueOf(getDateTimeToString(dateString)[2]);
        this.date = Integer.valueOf(getDateTimeToString(dateString)[3]);
        this.month = Integer.valueOf(getDateTimeToString(dateString)[4]);
        this.year = Integer.valueOf(getDateTimeToString(dateString)[5]);
        this.hours = Integer.valueOf(getDateTimeToString(dateString)[6]);
        this.minutes = Integer.valueOf(getDateTimeToString(dateString)[7]);
        this.seconds = Integer.valueOf(getDateTimeToString(dateString)[8]);
        this.timezoneOffset = Integer.valueOf(getDateTimeToString(dateString)[9]);
        this.method = method;
        this.endpoint = endpoint;
        this.protocolName = getProtocolToString(protocol)[0];
        this.protocolVersion = getProtocolToString(protocol)[1];
        this.responseCode = Integer.parseInt(responseCode);
        this.contentSize =  Long.parseLong(contentSize);
        this.link = link;
        this.mozillaName = getMozillaToString(mozilla)[0];
        this.mozillaVersion = getMozillaToString(mozilla)[1];
        this.osType = getOsToString(os)[0];
        this.osName = getOsToString(os)[1];
        this.osVersion = getOsToString(os)[2];
        this.webkitType = getWebkitToString(webkit)[0];
        this.webkitVersion = getWebkitToString(webkit)[1];
        this.renduHtmlName = getRenduHTMLToString(renduHtml)[0];
        this.renduHtmlType = getRenduHTMLToString(renduHtml)[1];
        this.chromeName = getChromeToString(chrome)[0];
        this.chromeVersion = getChromeToString(chrome)[1];
        this.safariName = getSafariToString(safari)[0];
        this.safariVersion = getSafariToString(safari)[1];
    }

    public ApacheAccessLog(ApacheAccessLog x) {
        this.id = x.getid();
        this.ip = x.getIp();
        this.countryCode = x.getCountryCode();
        this.countryName = x.getCountryName();
        this.regionCode = x.getRegionCode();
        this.regionName = x.getRegionName();
        this.city = x.getCity();
        this.postalCode = x.getPostalCode();
        this.lnglat = x.getLnglat();
        this.latitude = x.getLatitude();
        this.longitude = x.getLongitude();
        this.metroCode = x.getMetroCode();
        this.areaCode = x.getAreaCode();
        this.timezone = x.getTimezone();
        this.clientID = x.getClientID();
        this.userID = x.getUserID();
        this.dateTimeString = x.getDateTimeString();
        this.timestamp = x.getDateTimestamp();
        this.day = x.getDay();
        this.date = x.getDate();
        this.month = x.getMonth();
        this.year = x.getYear();
        this.hours = x.getHours();
        this.minutes = x.getMinutes();
        this.seconds = x.getSeconds();
        this.timezoneOffset = x.getTimezoneOffset();
        this.method = x.getMethod();
        this.endpoint = x.getEndpoint();
        this.protocolName = getProtocolName();
        this.protocolVersion = getProtocolVersion();
        this.responseCode = x.getResponseCode();
        this.contentSize =  x.getContentSize();
        this.link = x.getLink();
        this.mozillaName = x.getMozillaName();
        this.mozillaVersion = x.getMozillaVersion();
        this.osType = x.getOsType();
        this.osName = x.getOsName();
        this.osVersion = x.getOsVersion();
        this.webkitType = x.getWebkitType();
        this.webkitVersion = x.getWebkitVersion();
        this.renduHtmlName = x.getRenduHtmlName();
        this.renduHtmlType = x.getRenduHtmlType();
        this.chromeName = x.getChromeName();
        this.chromeVersion = x.getChromeVersion();
        this.safariName = x.getSafariName();
        this.safariVersion = x.getSafariVersion();
    }

    private int getid() {
        this.id = this.id + 1;
        return id;
    }

    public String getIp() {
        return ip;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public String getCountryName() {
        return countryName;
    }

    public String getRegionCode() {
        return regionCode;
    }

    public String getRegionName() {
        return regionName;
    }

    public String getCity() {
        return city;
    }

    public String getPostalCode() {
        return postalCode;
    }

    public List<Float> getLnglat() {
        return lnglat;
    }

    public Float getLatitude() {
        return latitude;
    }

    public Float getLongitude() {
        return longitude;
    }

    public int getMetroCode() {
        return metroCode;
    }

    public int getAreaCode() {
        return areaCode;
    }

    public String getTimezone() {
        return timezone;
    }

    public String getClientID() {
        return clientID;
    }

    public String getUserID() {
        return userID;
    }

    public String getDateTimeString() {
        return dateTimeString;
    }

    public String getDateTimestamp() { return timestamp; }

    public int getDay() {
        return day;
    }

    public int getDate() {
        return date;
    }

    public int getMonth() {
        return month;
    }

    public int getYear() {
        return year;
    }

    public int getHours() {
        return hours;
    }

    public int getMinutes() {
        return minutes;
    }

    public int getSeconds() {
        return seconds;
    }

    public int getTimezoneOffset() {
        return timezoneOffset;
    }

    public String getMethod() {
        return method;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getProtocolName() { return protocolName; }

    public String getProtocolVersion() {
        return protocolVersion;
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

    public String getMozillaName() { return mozillaName; }

    public String getMozillaVersion() {
        return mozillaVersion;
    }

    public String getOsType() {
        return osType;
    }

    public String getOsName() {
        return osName;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public String getWebkitType() {
        return webkitType;
    }

    public String getWebkitVersion() {
        return webkitVersion;
    }

    public String getRenduHtmlName() {
        return renduHtmlName;
    }

    public String getRenduHtmlType() {
        return renduHtmlType;
    }

    public String getChromeName() {
        return chromeName;
    }

    public String getChromeVersion() {
        return chromeVersion;
    }

    public String getSafariName() {
        return safariName;
    }

    public String getSafariVersion() {
        return safariVersion;
    }

    private void setid() {}

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public void setDateTimeString(String dateString) {
        this.dateTimeString = getDateTimeToString(dateString)[0];
        this.timestamp = getDateTimeToString(dateString)[1];
        this.day = Integer.valueOf(getDateTimeToString(dateString)[2]);
        this.date = Integer.valueOf(getDateTimeToString(dateString)[3]);
        this.month = Integer.valueOf(getDateTimeToString(dateString)[4]);
        this.year = Integer.valueOf(getDateTimeToString(dateString)[5]);
        this.hours = Integer.valueOf(getDateTimeToString(dateString)[6]);
        this.minutes = Integer.valueOf(getDateTimeToString(dateString)[7]);
        this.seconds = Integer.valueOf(getDateTimeToString(dateString)[8]);
        this.timezoneOffset = Integer.valueOf(getDateTimeToString(dateString)[9]);
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setProtocol(String protocol) {
        this.protocolName = getProtocolToString(protocol)[0];
        this.protocolVersion = getProtocolToString(protocol)[1];
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

    public void setMozilla(String mozilla) {
        this.mozillaName = getMozillaToString(mozilla)[0];
        this.mozillaVersion = getMozillaToString(mozilla)[1];
    }

    public void setOs(String os) {
        this.osType = getOsToString(os)[0];
        this.osName = getOsToString(os)[1];
        this.osVersion = getOsToString(os)[2];
    }

    public void setWebkit(String webkit) {
        this.webkitType = getWebkitToString(webkit)[0];
        this.webkitVersion = getWebkitToString(webkit)[1];
    }

    public void setRenduHtml(String renduHtml) {
        this.renduHtmlName = getRenduHTMLToString(renduHtml)[0];
        this.renduHtmlType = getRenduHTMLToString(renduHtml)[1];
    }

    public void setChrome(String chrome) {
        this.chromeName = getChromeToString(chrome)[0];
        this.chromeVersion = getChromeToString(chrome)[1];
    }

    public void setSafari(String safari) {
        this.safariName = getSafariToString(safari)[0];
        this.safariVersion = getSafariToString(safari)[1];
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

    private String[] getProtocolToString(String protocol) {
        Pattern pro = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher mpro = pro.matcher(protocol);

        if (!mpro.find()) {
            logger.log(Level.ALL, "Cannot parse protocol " + protocol);
            throw new RuntimeException("Error parsing protocol");
        }
        return new String[]{mpro.group(1), mpro.group(2)};
    }

    private String[] getWebkitToString(String webkit) {
        Pattern wk = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher mwk = wk.matcher(webkit);

        if (!mwk.find()) {
            logger.log(Level.ALL, "Cannot parse webkit " + webkit);
            throw new RuntimeException("Error parsing webkit");
        }

        return new String[]{mwk.group(1), mwk.group(2)};
    }

    private String[] getRenduHTMLToString(String renduHtml) {
        Pattern rendu = Pattern.compile("^(\\S+), (\\S+) (\\S+)");
        Matcher mrendu = rendu.matcher(renduHtml);

        if (!mrendu.find()) {
            logger.log(Level.ALL, "Cannot parse renduHtml " + renduHtml);
            throw new RuntimeException("Error parsing renduHtml");
        }

        return new String[]{mrendu.group(1), mrendu.group(3)};
    }

    private String[] getChromeToString(String chrome) {
        Pattern chr = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher mchr = chr.matcher(chrome);

        if (!mchr.find()) {
            logger.log(Level.ALL, "Cannot parse chrome " + chrome);
            throw new RuntimeException("Error parsing chrome");
        }

        return new String[]{mchr.group(1), mchr.group(2)};
    }

    private String[] getSafariToString(String safari) {
        Pattern saf = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher msaf = saf.matcher(safari);

        if (!msaf.find()) {
            logger.log(Level.ALL, "Cannot parse safari " + safari);
            throw new RuntimeException("Error parsing safari");
        }

        return new String[]{msaf.group(1), msaf.group(2)};
    }

    private String[] getMozillaToString(String mozilla) {
        Pattern moz = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher mmoz = moz.matcher(mozilla);

        if (!mmoz.find()) {
            logger.log(Level.ALL, "Cannot parse mozilla " + mozilla);
            throw new RuntimeException("Error parsing mozilla");
        }

        return new String[]{mmoz.group(1), mmoz.group(2)};
    }

    private String[] getOsToString(String os) {
        Pattern pos = Pattern.compile("^(\\S+); (.*) (\\S+)");
        Matcher mos = pos.matcher(os);

        if (!mos.find()) {
            logger.log(Level.ALL, "Cannot parse os " + os);
            throw new RuntimeException("Error parsing os");
        }

        return new String[]{mos.group(1), mos.group(2), mos.group(3)};
    }

    private String[] getDateTimeToString(String dateString) {

        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss ZZZ", Locale.US);
        Date date = null;

        try {
            date = formatter.parse(dateString);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return new String[]{dateString, String.valueOf(date.getTime()), String.valueOf(date.getDay()),
                String.valueOf(date.getDate()), String.valueOf(date.getMonth()),
                String.valueOf(date.getYear() + 1900), String.valueOf(date.getHours()),
                String.valueOf(date.getMinutes()), String.valueOf(date.getSeconds()),
                String.valueOf(date.getTimezoneOffset())};
    }

    @Override
    public String toString() {
        return String.format(//"{" +
                        "\"id\" : %d, " +
                        "\"ip\" : %s, " +
                        "\"countryCode\" : %s, " +
                        "\"countryName\" : %s, " +
                        "\"regionCode\" : %s, " +
                        "\"regionName\" : %s, " +
                        "\"city\" : %s, " +
                        "\"postalCode\" : %s, " +
                        "\"latitude\" : %s, " +
                        "\"longitude\" : %s, " +
                        "\"metroCode\" : %s, " +
                        "\"areaCode\" : %s, " +
                        "\"timezone\" : %s, " +
                        "\"clientID\" : \"%s\", " +
                        "\"userID\" : \"%s\", " +
                        "\"dateTimeString\" : %s, " +
                        "\"timestamp\" : %s, " +
                        "\"day\" : %s, " +
                        "\"date\" : %s, " +
                        "\"month\" : %s, " +
                        "\"year\" : %s, " +
                        "\"hours\" : %s, " +
                        "\"minutes\" : %s, " +
                        "\"seconds\" : %s, " +
                        "\"timezoneOffset\" : %s, " +
                        "\"method\" : \"%s\", " +
                        "\"endPoint\" : \"%s\", " +
                        "\"protocolName\" : %s, " +
                        "\"protocolVersion\" : %s, " +
                        "\"responseCode\" : \"%s\", " +
                        "\"contentSize\" : \"%s\", " +
                        "\"link\" : \"%s\", " +
                        "\"mozillaName\" : %s, " +
                        "\"mozillaVersion\" : %s, " +
                        "\"osType\" : %s, " +
                        "\"osName\" : %s, " +
                        "\"osVersion\" : %s, " +
                        "\"webkitType\" : %s, " +
                        "\"webkitVersion\" : %s, " +
                        "\"renduHtmlName\" : %s, " +
                        "\"renduHtmlType\" : %s, " +
                        "\"chromeName\" : %s, " +
                        "\"chromeVersion\" : %s, " +
                        "\"safariName\" : %s, " +
                        "\"safariVersion\" : %s"// +
                       ,// "}",
                id, ip, countryCode, countryName, regionCode, regionName, city, postalCode,
                latitude, longitude, metroCode, areaCode, timezone, clientID, userID, dateTimeString,
                timestamp, day, date, month, year, hours, minutes, seconds, timezoneOffset,
                method, endpoint, protocolName, protocolVersion, responseCode, contentSize, link,
                mozillaName, mozillaVersion, osType, osName, osVersion, webkitType, webkitVersion,
                renduHtmlName, renduHtmlType, chromeName, chromeVersion, safariName, safariVersion
        );

    }

    public XContentBuilder toJSON() throws IOException {

        return jsonBuilder()
                .startObject()
                .field("id", id)
                .field("ip", ip)
                .field("countryCode", countryCode)
                .field("countryName", countryName)
                .field("regionCode", regionCode)
                .field("regionName", regionName)
                .field("city", city)
                .field("postalCode", postalCode)
                .field("lnglat", lnglat)
                .field("latitude", latitude)
                .field("longitude", longitude)
                .field("metroCode", metroCode)
                .field("areaCode", areaCode)
                .field("timezone", timezone)
                .field("clientID", clientID)
                .field("userID", userID)
                .field("dateTimeString", dateTimeString)
                .field("timestamp", timestamp)
                .field("day", day)
                .field("date", date)
                .field("month", month)
                .field("year", year)
                .field("hours", hours)
                .field("minutes", minutes)
                .field("seconds", seconds)
                .field("timezoneOffset", timezoneOffset)
                .field("method", method)
                .field("endPoint", endpoint)
                .field("protocolName", protocolName)
                .field("protocolVersion", protocolVersion)
                .field("responseCode", responseCode)
                .field("contentSize", contentSize)
                .field("link", link)
                .field("mozillaName", mozillaName)
                .field("mozillaVersion", mozillaVersion)
                .field("osType", osType)
                .field("osName", osName)
                .field("osVersion", osVersion)
                .field("webkitType", webkitType)
                .field("webkitVersion", webkitVersion)
                .field("renduHtmlName", renduHtmlName)
                .field("renduHtmlType", renduHtmlType)
                .field("chromeName", chromeName)
                .field("chromeVersion", chromeVersion)
                .field("safariName", safariName)
                .field("safariVersion", safariVersion)
                .endObject();
    }

}