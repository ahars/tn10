package formatLog;

import org.apache.cassandra.utils.UUIDGen;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ApacheAccessLog implements Serializable {

    private static final Logger logger = Logger.getLogger("Access");

    private UUID id = null;
    private String ip = null;
    private String country_code = null;
    private String country_name = null;
    private String region_code = null;
    private String region_name = null;
    private String city = null;
    private String postal_code = null;
    private List<Float> lnglat = null;
    private Float latitude = null;
    private Float longitude = null;
    private Integer metro_code = null;
    private Integer area_code = null;
    private String timezone = null;
    private String client_id = null;
    private String user_id = null;
    private String date_time_string = null;
    private String timestamp = null;
    private Integer day = null;
    private Integer date = null;
    private Integer month = null;
    private Integer year = null;
    private Integer hours = null;
    private Integer minutes = null;
    private Integer seconds = null;
    private Integer timezone_offset = null;
    private String method = null;
    private String endpoint = null;
    private String protocol_name = null;
    private String protocol_version = null;
    private Integer response_code = null;
    private Integer content_size = null;
    private String link = null;
    private String mozilla_name = null;
    private String mozilla_version = null;
    private String os_type = null;
    private String os_name = null;
    private String os_version = null;
    private String webkit_type = null;
    private String webkit_version = null;
    private String rendu_html_name = null;
    private String rendu_html_type = null;
    private String chrome_name = null;
    private String chrome_version = null;
    private String safari_name = null;
    private String safari_version = null;

    public ApacheAccessLog(String ip, String client_id, String user_id, String date_string,
                   String method, String endpoint, String protocol, String response_code,
                   String content_size, String link, String mozilla, String os,
                   String webkit, String rendu_html, String chrome, String safari) {

        LocationIp location = new LocationIp(ip);

        this.id = UUIDGen.getTimeUUID();
        this.ip = location.getIp();
        this.country_code = location.getCountryCode();
        this.country_name = location.getCountryName();
        this.region_code = location.getRegionCode();
        this.region_name = location.getRegionName();
        this.city = location.getCity();
        this.postal_code = location.getPostalCode();
        this.lnglat = location.getLnglat();
        this.latitude = location.getLatitude();
        this.longitude = location.getLongitude();
        this.metro_code = location.getMetroCode();
        this.area_code = location.getAreaCode();
        this.timezone = location.getTimezone();
        this.client_id = client_id;
        this.user_id = user_id;
        this.date_time_string = getDate_timeToString(date_string)[0];
        this.timestamp = getDate_timeToString(date_string)[1];
        this.day = Integer.valueOf(getDate_timeToString(date_string)[2]);
        this.date = Integer.valueOf(getDate_timeToString(date_string)[3]);
        this.month = Integer.valueOf(getDate_timeToString(date_string)[4]);
        this.year = Integer.valueOf(getDate_timeToString(date_string)[5]);
        this.hours = Integer.valueOf(getDate_timeToString(date_string)[6]);
        this.minutes = Integer.valueOf(getDate_timeToString(date_string)[7]);
        this.seconds = Integer.valueOf(getDate_timeToString(date_string)[8]);
        this.timezone_offset = Integer.valueOf(getDate_timeToString(date_string)[9]);
        this.method = method;
        this.endpoint = endpoint;
        this.protocol_name = getProtocolToString(protocol)[0];
        this.protocol_version = getProtocolToString(protocol)[1];
        this.response_code = Integer.parseInt(response_code);
        this.content_size =  Integer.parseInt(content_size);
        this.link = link;
        this.mozilla_name = getMozillaToString(mozilla)[0];
        this.mozilla_version = getMozillaToString(mozilla)[1];
        this.os_type = getOsToString(os)[0];
        this.os_name = getOsToString(os)[1];
        this.os_version = getOsToString(os)[2];
        this.webkit_type = getWebkitToString(webkit)[0];
        this.webkit_version = getWebkitToString(webkit)[1];
        this.rendu_html_name = getRendu_htmlToString(rendu_html)[0];
        this.rendu_html_type = getRendu_htmlToString(rendu_html)[1];
        this.chrome_name = getChromeToString(chrome)[0];
        this.chrome_version = getChromeToString(chrome)[1];
        this.safari_name = getSafariToString(safari)[0];
        this.safari_version = getSafariToString(safari)[1];
    }

    public ApacheAccessLog(ApacheAccessLog log) {

        this.id = log.getId();
        this.ip = log.getIp();
        this.country_code = log.getCountry_code();
        this.country_name = log.getCountry_name();
        this.region_code = log.getRegion_code();
        this.region_name = log.getRegion_name();
        this.city = log.getCity();
        this.postal_code = log.getPostal_code();
        this.lnglat = log.getLnglat();
        this.latitude = log.getLatitude();
        this.longitude = log.getLongitude();
        this.metro_code = log.getMetro_code();
        this.area_code = log.getArea_code();
        this.timezone = log.getTimezone();
        this.client_id = log.getClient_id();
        this.user_id = log.getUser_id();
        this.date_time_string = log.getDate_time_string();
        this.timestamp = log.getTimestamp();
        this.day = log.getDay();
        this.date = log.getDate();
        this.month = log.getMonth();
        this.year = log.getYear();
        this.hours = log.getHours();
        this.minutes = log.getMinutes();
        this.seconds = log.getSeconds();
        this.timezone_offset = log.getTimezone_offset();
        this.method = log.getMethod();
        this.endpoint = log.getEndpoint();
        this.protocol_name = log.getProtocol_name();
        this.protocol_version = log.getProtocol_version();
        this.response_code = log.getResponse_code();
        this.content_size =  log.getContent_size();
        this.link = log.getLink();
        this.mozilla_name = log.getMozilla_name();
        this.mozilla_version = log.getMozilla_version();
        this.os_type = log.getOs_type();
        this.os_name = log.getOs_name();
        this.os_version = log.getOs_version();
        this.webkit_type = log.getWebkit_type();
        this.webkit_version = log.getWebkit_version();
        this.rendu_html_name = log.getRendu_html_name();
        this.rendu_html_type = log.getRendu_html_type();
        this.chrome_name = log.getChrome_name();
        this.chrome_version = log.getChrome_version();
        this.safari_name = log.getSafari_name();
        this.safari_version = log.getSafari_version();
    }

    public ApacheAccessLog(String id, String area_code, String chrome_name, String chrome_version, String city,
                           String client_id, String content_size, String country_code, String country_name,
                           String date, String date_time_string, String day, String endpoint, String hours,
                           String ip, String latitude, String link, String lnglat, String longitude, String method,
                           String metro_code, String minutes, String month, String mozilla_name, String mozilla_version,
                           String os_name, String os_type, String os_version, String postal_code, String protocol_name,
                           String protocol_version, String region_code, String region_name, String rendu_html_name,
                           String rendu_html_type, String response_code, String safari_name, String safari_version,
                           String seconds, String timestamp, String timezone, String timezone_offset, String user_id,
                           String webkit_type, String webkit_version, String year) {

        this.id = UUID.fromString(id);
        this.ip = ip;
        this.country_code = country_code;
        this.country_name = country_name;
        this.region_code = region_code;
        this.region_name = region_name;
        this.city = city;
        this.postal_code = postal_code;
        this.latitude = Float.parseFloat(latitude);
        this.longitude = Float.parseFloat(longitude);
        this.lnglat = new ArrayList<Float>();
        this.lnglat.add(this.longitude);
        this.lnglat.add(this.latitude);
        this.metro_code = Integer.getInteger(metro_code);
        this.area_code = Integer.getInteger(area_code);
        this.timezone = timezone;
        this.client_id = client_id;
        this.user_id = user_id;
        this.date_time_string = date_time_string;
        this.timestamp = timestamp;
        this.day = Integer.valueOf(day);
        this.date = Integer.valueOf(date);
        this.month = Integer.valueOf(month);
        this.year = Integer.valueOf(year);
        this.hours = Integer.valueOf(hours);
        this.minutes = Integer.valueOf(minutes);
        this.seconds = Integer.valueOf(seconds);
        this.timezone_offset = Integer.valueOf(timezone_offset);
        this.method = method;
        this.endpoint = endpoint;
        this.protocol_name = protocol_name;
        this.protocol_version = protocol_version;
        this.response_code = Integer.parseInt(response_code);
        this.content_size = Integer.parseInt(content_size);
        this.link = link;
        this.mozilla_name = mozilla_name;
        this.mozilla_version = mozilla_version;
        this.os_type = os_type;
        this.os_name = os_name;
        this.os_version = os_version;
        this.webkit_type = webkit_type;
        this.webkit_version = webkit_version;
        this.rendu_html_name = rendu_html_name;
        this.rendu_html_type = rendu_html_type;
        this.chrome_name = chrome_name;
        this.chrome_version = chrome_version;
        this.safari_name = safari_name;
        this.safari_version = safari_version;
    }


    public UUID getId() { return id; }
    public void setId() { this.id = UUIDGen.getTimeUUID(); }

    public String getIp() { return ip; }
    public void setIp(String ip) { this.ip = ip; }

    public String getCountry_code() { return country_code; }
    public void setCountry_code(String country_code) { this.country_code = country_code; }

    public String getCountry_name() { return country_name; }
    public void setCountry_name(String country_name) { this.country_name = country_name; }

    public String getRegion_code() { return region_code; }
    public void setRegion_code(String region_code) { this.region_code = region_code; }

    public String getRegion_name() { return region_name; }
    public void setRegion_name(String region_name) { this.region_name = region_name; }

    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public String getPostal_code() { return postal_code; }
    public void setPostal_code(String postal_code) { this.postal_code = postal_code; }

    public List<Float> getLnglat() { return lnglat; }
    public void setLnglat(List<Float> lnglat) { this.lnglat = lnglat; }

    public Float getLatitude() { return latitude; }
    public void setLatitude(Float latitude) { this.latitude = latitude; }

    public Float getLongitude() { return longitude; }
    public void setLongitude(Float longitude) { this.longitude = longitude; }

    public Integer getMetro_code() { return metro_code; }
    public void setMetro_code(Integer metro_code) { this.metro_code = metro_code; }

    public Integer getArea_code() { return area_code; }
    public void setArea_code(Integer area_code) { this.area_code = area_code; }

    public String getTimezone() { return timezone; }
    public void setTimezone(String timezone) { this.timezone = timezone; }

    public String getClient_id() { return client_id; }
    public void setClient_id(String client_id) { this.client_id = client_id; }

    public String getUser_id() { return user_id; }
    public void setUser_id(String user_id) { this.user_id = user_id; }

    public String getDate_time_string() { return date_time_string; }
    public void setDate_time_string(String date_time_string) { this.date_time_string = date_time_string; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public Integer getDay() { return day; }
    public void setDay(Integer day) { this.day = day; }

    public Integer getDate() { return date; }
    public void setDate(Integer date) { this.date = date; }

    public Integer getMonth() { return month; }
    public void setMonth(Integer month) { this.month = month; }

    public Integer getYear() { return year; }
    public void setYear(Integer year) { this.year = year; }

    public Integer getHours() { return hours; }
    public void setHours(Integer hours) { this.hours = hours; }

    public Integer getMinutes() { return minutes; }
    public void setMinutes(Integer minutes) { this.minutes = minutes; }

    public Integer getSeconds() { return seconds; }
    public void setSeconds(Integer seconds) { this.seconds = seconds; }

    public Integer getTimezone_offset() { return timezone_offset; }
    public void setTimezone_offset(Integer timezone_offset) { this.timezone_offset = timezone_offset; }

    public String getMethod() { return method; }
    public void setMethod(String method) { this.method = method; }

    public String getEndpoint() { return endpoint; }
    public void setEndpoint(String endpoint) { this.endpoint = endpoint; }

    public String getProtocol_name() { return protocol_name; }
    public void setProtocol_name(String protocol_name) { this.protocol_name = protocol_name; }

    public String getProtocol_version() { return protocol_version; }
    public void setProtocol_version(String protocol_version) { this.protocol_version = protocol_version; }

    public Integer getResponse_code() { return response_code; }
    public void setResponse_code(Integer response_code) { this.response_code = response_code; }

    public Integer getContent_size() { return content_size; }
    public void setContent_size(Integer content_size) { this.content_size = content_size; }

    public String getLink() { return link; }
    public void setLink(String link) { this.link = link; }

    public String getMozilla_name() { return mozilla_name; }
    public void setMozilla_name(String mozilla_name) { this.mozilla_name = mozilla_name; }

    public String getMozilla_version() { return mozilla_version; }
    public void setMozilla_version(String mozilla_version) { this.mozilla_version = mozilla_version; }

    public String getOs_type() { return os_type; }
    public void setOs_type(String os_type) { this.os_type = os_type; }

    public String getOs_name() { return os_name; }
    public void setOs_name(String os_name) { this.os_name = os_name; }

    public String getOs_version() { return os_version; }
    public void setOs_version(String os_version) { this.os_version = os_version; }

    public String getWebkit_type() { return webkit_type; }
    public void setWebkit_type(String webkit_type) { this.webkit_type = webkit_type; }

    public String getWebkit_version() { return webkit_version; }
    public void setWebkit_version(String webkit_version) { this.webkit_version = webkit_version; }

    public String getRendu_html_name() { return rendu_html_name; }
    public void setRendu_html_name(String rendu_html_name) { this.rendu_html_name = rendu_html_name; }

    public String getRendu_html_type() { return rendu_html_type; }
    public void setRendu_html_type(String rendu_html_type) { this.rendu_html_type = rendu_html_type; }

    public String getChrome_name() { return chrome_name; }
    public void setChrome_name(String chrome_name) { this.chrome_name = chrome_name; }

    public String getChrome_version() { return chrome_version; }
    public void setChrome_version(String chrome_version) { this.chrome_version = chrome_version; }

    public String getSafari_name() { return safari_name; }
    public void setSafari_name(String safari_name) { this.safari_name = safari_name; }

    public String getSafari_version() { return safari_version; }
    public void setSafari_version(String safari_version) { this.safari_version = safari_version; }

    private String[] getDate_timeToString(String dateString) {
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss ZZZ", Locale.US);
        Date date = null;

        try {
            date = formatter.parse(dateString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return new String[]{dateString, String.valueOf(date.getTime()), String.valueOf(date.getDay()),
                String.valueOf(date.getDate()), String.valueOf(date.getMonth() + 1),
                String.valueOf(date.getYear() + 1900), String.valueOf(date.getHours()),
                String.valueOf(date.getMinutes()), String.valueOf(date.getSeconds()),
                String.valueOf(date.getTimezoneOffset())};
    }

    public void setDate_time(String date_string) {
        this.date_time_string = getDate_timeToString(date_string)[0];
        this.timestamp = getDate_timeToString(date_string)[1];
        this.day = Integer.valueOf(getDate_timeToString(date_string)[2]);
        this.date = Integer.valueOf(getDate_timeToString(date_string)[3]);
        this.month = Integer.valueOf(getDate_timeToString(date_string)[4]);
        this.year = Integer.valueOf(getDate_timeToString(date_string)[5]);
        this.hours = Integer.valueOf(getDate_timeToString(date_string)[6]);
        this.minutes = Integer.valueOf(getDate_timeToString(date_string)[7]);
        this.seconds = Integer.valueOf(getDate_timeToString(date_string)[8]);
        this.timezone_offset = Integer.valueOf(getDate_timeToString(date_string)[9]);
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

    public void setProtocol(String protocol) {
        this.protocol_name = getProtocolToString(protocol)[0];
        this.protocol_version = getProtocolToString(protocol)[1];
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

    public void setMozilla(String mozilla) {
        this.mozilla_name = getMozillaToString(mozilla)[0];
        this.mozilla_version = getMozillaToString(mozilla)[1];
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

    public void setOs(String os) {
        this.os_type = getOsToString(os)[0];
        this.os_name = getOsToString(os)[1];
        this.os_version = getOsToString(os)[2];
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

    public void setWebkit(String webkit) {
        this.webkit_type = getWebkitToString(webkit)[0];
        this.webkit_version = getWebkitToString(webkit)[1];
    }

    private String[] getRendu_htmlToString(String rendu_html) {
        Pattern rendu = Pattern.compile("^(\\S+), (\\S+) (\\S+)");
        Matcher mrendu = rendu.matcher(rendu_html);

        if (!mrendu.find()) {
            logger.log(Level.ALL, "Cannot parse rendu_html " + rendu_html);
            throw new RuntimeException("Error parsing rendu_html");
        }
        return new String[]{mrendu.group(1), mrendu.group(3)};
    }

    public void setRenduHtml(String renduHtml) {
        this.rendu_html_name = getRendu_htmlToString(renduHtml)[0];
        this.rendu_html_type = getRendu_htmlToString(renduHtml)[1];
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

    public void setChrome(String chrome) {
        this.chrome_name = getChromeToString(chrome)[0];
        this.chrome_version = getChromeToString(chrome)[1];
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

    public void setSafari(String safari) {
        this.safari_name = getSafariToString(safari)[0];
        this.safari_version = getSafariToString(safari)[1];
    }

    public String toString() {
        return String.format("id = %d, ip = %s, country_code = %s, country_name = %s, region_code = %s, " +
                        "region_name = %s, city = %s, postal_code = %s, lnglat = %s, latitude = %s, longitude = %s, " +
                        "metro_code = %s, area_code = %s, client_id = %s, user_id = %s, date_time_string = %s, " +
                        "timestamp = %s, day = %s, date = %s, month = %s, year = %s, hours = %s, minutes = %s, " +
                        "seconds = %s, timezone_offset = %s, method = %s, endpoint = %s, protocol_name = %s, " +
                        "protocol_version = %s, response_code = %s, content_size = %s, link = %s, mozilla_name = %s, " +
                        "mozilla_version = %s, os_type = %s, os_name = %s, os_version = %s, webkit_type = %s, " +
                        "webkit_version = %s, rendu_html_name = %s, rendu_html_type = %s, chrome_name = %s, " +
                        "chrome_version = %s, safari_name = %s, safari_version = %s",
                id.hashCode(), ip, country_code, country_name, region_code, region_name, city, postal_code, lnglat,
                latitude, longitude, metro_code, area_code, client_id, user_id, date_time_string, timestamp, day,
                date, month, year, hours, minutes, seconds, timezone_offset, method, endpoint, protocol_name,
                protocol_version, response_code, content_size, link, mozilla_name, mozilla_version, os_type, os_name,
                os_version, webkit_type, webkit_version, rendu_html_name, rendu_html_type, chrome_name, chrome_version,
                safari_name, safari_version);
    }

    public XContentBuilder toJSON() throws IOException {
        return jsonBuilder()
                .startObject()
                .field("id", id)
                .field("ip", ip)
                .field("country_code", country_code)
                .field("country_name", country_name)
                .field("region_code", region_code)
                .field("region_name", region_name)
                .field("city", city)
                .field("postal_code", postal_code)
                .field("lnglat", lnglat)
                .field("latitude", latitude)
                .field("longitude", longitude)
                .field("metro_code", metro_code)
                .field("area_code", area_code)
                .field("timezone", timezone)
                .field("client_id", client_id)
                .field("user_id", user_id)
                .field("date_time_string", date_time_string)
                .field("timestamp", timestamp)
                .field("day", day)
                .field("date", date)
                .field("month", month)
                .field("year", year)
                .field("hours", hours)
                .field("minutes", minutes)
                .field("seconds", seconds)
                .field("timezone_offset", timezone_offset)
                .field("method", method)
                .field("endPoint", endpoint)
                .field("protocol_name", protocol_name)
                .field("protocol_version", protocol_version)
                .field("response_code", response_code)
                .field("content_size", content_size)
                .field("link", link)
                .field("mozilla_name", mozilla_name)
                .field("mozilla_version", mozilla_version)
                .field("os_type", os_type)
                .field("os_name", os_name)
                .field("os_version", os_version)
                .field("webkit_type", webkit_type)
                .field("webkit_version", webkit_version)
                .field("rendu_html_name", rendu_html_name)
                .field("rendu_html_type", rendu_html_type)
                .field("chrome_name", chrome_name)
                .field("chrome_version", chrome_version)
                .field("safari_name", safari_name)
                .field("safari_version", safari_version)
                .endObject();
    }
}
