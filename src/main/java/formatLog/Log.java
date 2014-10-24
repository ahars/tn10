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

public class Log implements Serializable {

    private static final Logger logger = Logger.getLogger("Log");

    private UUID id = null;
    private String ip_adress = null;
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
    private HashMap<String, String> date_time = null;
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
    private String others = null;

    public Log(String ip_adress, String client_id, String user_id, String date_string, String method, String endpoint,
               String protocol, String response_code, String content_size, String others) {

        LocationIp location = new LocationIp(ip_adress);

        this.id = UUIDGen.getTimeUUID();
        this.ip_adress = location.getIp();
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

        this.date_time = new HashMap<>();
        this.date_time.put("date_time", getDate_timeToString(date_string)[0]);
        this.date_time.put("timestamp", getDate_timeToString(date_string)[1]);
        this.date_time.put("day", getDate_timeToString(date_string)[2]);
        this.date_time.put("date", getDate_timeToString(date_string)[3]);
        this.date_time.put("month", getDate_timeToString(date_string)[4]);
        this.date_time.put("year", getDate_timeToString(date_string)[5]);
        this.date_time.put("hours", getDate_timeToString(date_string)[6]);
        this.date_time.put("minutes", getDate_timeToString(date_string)[7]);
        this.date_time.put("seconds", getDate_timeToString(date_string)[8]);
        this.date_time.put("timezone_offset", getDate_timeToString(date_string)[9]);

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
        this.others = others;
    }

    public Log(Log log) {

        this.id = log.getId();
        this.ip_adress = log.getIp_adress();
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

        this.date_time = log.getDate_time();

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
        this.others = log.getOthers();
    }

    public Log(String id, String area_code, String city, String client_id, String content_size, String country_code,
               String country_name, String date, String date_time, String day, String endpoint, String hours,
               String ip_adress, String latitude,String lnglat, String longitude, String method, String metro_code,
               String minutes, String month, String others, String postal_code, String protocol_name,
               String protocol_version, String region_code, String region_name, String response_code, String seconds,
               String timestamp, String timezone, String timezone_offset, String user_id, String year) {

        this.id = UUID.fromString(id);
        this.ip_adress = ip_adress;
        this.country_code = country_code;
        this.country_name = country_name;
        this.region_code = region_code;
        this.region_name = region_name;
        this.city = city;
        this.postal_code = postal_code;
        this.latitude = Float.parseFloat(latitude);
        this.longitude = Float.parseFloat(longitude);
        this.lnglat = new ArrayList<>();
        this.lnglat.add(this.longitude);
        this.lnglat.add(this.latitude);
        this.metro_code = Integer.getInteger(metro_code);
        this.area_code = Integer.getInteger(area_code);
        this.timezone = timezone;
        this.client_id = client_id;
        this.user_id = user_id;

        this.date_time = new HashMap<>();
        this.date_time.put("date_time", date_time);
        this.date_time.put("timestamp", timestamp);
        this.date_time.put("day", day);
        this.date_time.put("date", date);
        this.date_time.put("month", month);
        this.date_time.put("year", year);
        this.date_time.put("hours", hours);
        this.date_time.put("minutes", minutes);
        this.date_time.put("seconds", seconds);
        this.date_time.put("timezone_offset", timezone_offset);

        this.date_time_string = date_time;
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
        this.others = others;
    }

    public UUID getId() { return id; }
    public void setId() { this.id = UUIDGen.getTimeUUID(); }

    public String getIp_adress() { return ip_adress; }
    public void setIp_adress(String ip_adress) { this.ip_adress = ip_adress; }

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

    public String getOthers() { return others; }
    public void setOthers(String others) { this.others = others; }

    public HashMap getDate_time() { return date_time; }
    public void setDate_time(HashMap date_time) { this.date_time = date_time; }

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

    private String[] getProtocolToString(String protocol) {
        Pattern pro = Pattern.compile("^(\\S+)/(\\S+)");
        Matcher mpro = pro.matcher(protocol);

        if (!mpro.find()) {
            logger.log(Level.ALL, "Cannot parse protocol " + protocol);
            throw new RuntimeException("Error parsing protocol");
        }
        return new String[]{mpro.group(1), mpro.group(2)};
    }

    public String toString() {
        return String.format("id = %d, ip_adress = %s, country_code = %s, country_name = %s, region_code = %s, " +
                        "region_name = %s, city = %s, postal_code = %s, lnglat = %s, latitude = %s, longitude = %s, " +
                        "metro_code = %s, area_code = %s, client_id = %s, user_id = %s, date_time_string = %s, " +
                        "timestamp = %s, day = %s, date = %s, month = %s, year = %s, hours = %s, minutes = %s, " +
                        "seconds = %s, timezone_offset = %s, method = %s, endpoint = %s, protocol_name = %s, " +
                        "protocol_version = %s, response_code = %s, content_size = %s, others = {%s}",
                id.hashCode(), ip_adress, country_code, country_name, region_code, region_name, city, postal_code, lnglat,
                latitude, longitude, metro_code, area_code, client_id, user_id, date_time_string, timestamp, day,
                date, month, year, hours, minutes, seconds, timezone_offset, method, endpoint, protocol_name,
                protocol_version, response_code, content_size, others);
    }

    public XContentBuilder toJSON() throws IOException {
        return jsonBuilder()
                .startObject()
                .field("id", id)
                .field("ip_adress", ip_adress)
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
                .field("date_time_string", date_time.get("date_time"))
                .field("timestamp", date_time.get("timestamp"))
                .field("day", Integer.getInteger(date_time.get("day")))
                .field("date", Integer.getInteger(date_time.get("date")))
                .field("month", Integer.getInteger(date_time.get("month")))
                .field("year", Integer.getInteger(date_time.get("year")))
                .field("hours", Integer.getInteger(date_time.get("hours")))
                .field("minutes", Integer.getInteger(date_time.get("minutes")))
                .field("seconds", Integer.getInteger(date_time.get("seconds")))
                .field("timezone_offset", Integer.getInteger(date_time.get("timezone_offset")))
                .field("method", method)
                .field("endPoint", endpoint)
                .field("protocol_name", protocol_name)
                .field("protocol_version", protocol_version)
                .field("response_code", response_code)
                .field("content_size", content_size)
                .field("others", others)
                .endObject();
    }
}
