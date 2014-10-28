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
    private HashMap<String, String> ip = null;
    private List<Float> lnglat = null;
    private String client_id = null;
    private String user_id = null;
    private HashMap<String, String> date_time = null;
    private String method = null;
    private String endpoint = null;
    private String protocol_name = null;
    private String protocol_version = null;
    private Integer response_code = null;
    private Integer content_size = null;
    private String others = null;

    public Log(String id, String client_id, String content_size, String date_time, String endpoint, String ip,
               String lnglat, String method, String others, String protocol_name, String protocol_version,
               String response_code, String user_id) {

        this.id = UUID.fromString(id);
        this.ip = getIpFromString(ip);
        this.lnglat = getLnglatFromString(lnglat);
        this.client_id = client_id;
        this.user_id = user_id;
        this.date_time = getDateFromString(date_time);
        this.method = method;
        this.endpoint = endpoint;
        this.protocol_name = protocol_name;
        this.protocol_version = protocol_version;
        this.response_code = Integer.parseInt(response_code);
        this.content_size = Integer.parseInt(content_size);
        this.others = others;
    }

    public Log(UUID id, String client_id, Integer content_size, HashMap<String, String> date_time, String endpoint,
               HashMap<String, String> ip, List<Float> lnglat, String method, String others, String protocol_name,
               String protocol_version, Integer response_code, String user_id) {

        this.id = id;
        this.client_id = client_id;
        this.content_size = content_size;
        this.date_time.putAll(date_time);
        this.endpoint = endpoint;
        this.ip.putAll(ip);
        this.lnglat.addAll(lnglat);
        this.method = method;
        this.others = others;
        this.protocol_name = protocol_name;
        this.protocol_version = protocol_version;
        this.response_code = response_code;
        this.user_id = user_id;

    }

    public Log(String ip_adress, String client_id, String user_id, String date_string, String method, String endpoint,
               String protocol, String response_code, String content_size, String others) {

        LocationIpOther location = new LocationIpOther(ip_adress);

        this.id = UUIDGen.getTimeUUID();
        this.ip = location.getIp_adress();
        this.lnglat = location.getLnglat();

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
        this.ip = log.getIp();
        this.lnglat = log.getLnglat();
        this.client_id = log.getClient_id();
        this.user_id = log.getUser_id();

        this.date_time = log.getDate_time();

        this.method = log.getMethod();
        this.endpoint = log.getEndpoint();
        this.protocol_name = log.getProtocol_name();
        this.protocol_version = log.getProtocol_version();
        this.response_code = log.getResponse_code();
        this.content_size =  log.getContent_size();
        this.others = log.getOthers();
    }

    public UUID getId() { return id; }
    public void setId() { this.id = UUIDGen.getTimeUUID(); }

    public HashMap getIp() { return ip; }
    public void setIp(HashMap<String, String> ip) { this.ip = ip; }

    public List<Float> getLnglat() { return lnglat; }
    public void setLnglat(List<Float> lnglat) { this.lnglat = lnglat; }

    public String getClient_id() { return client_id; }
    public void setClient_id(String client_id) { this.client_id = client_id; }

    public String getUser_id() { return user_id; }
    public void setUser_id(String user_id) { this.user_id = user_id; }

    public HashMap<String, String> getDate_time() { return date_time; }
    public void setDate_time_string(HashMap<String, String> date_time) { this.date_time = date_time; }

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
        return String.format("id = %d, ip = %s, lnglat = %s, client_id = %s, user_id = %s, date_time = %s, " +
                        "method = %s, endpoint = %s, protocol_name = %s, " + "protocol_version = %s, " +
                        "response_code = %s, content_size = %s, others = %s",
                id.hashCode(), ip, lnglat, client_id, user_id, date_time, method, endpoint, protocol_name,
                protocol_version, response_code, content_size, others);
    }

    public XContentBuilder toJSON() throws IOException {
        return jsonBuilder()
                .startObject()
                .field("id", id)
                .field("ip_adress", ip.get("ip_adress"))
                .field("country_code", ip.get("country_code"))
                .field("country_name", ip.get("country_name"))
                .field("region_code", ip.get("region_code"))
                .field("region_name", ip.get("region_name"))
                .field("city", ip.get("city"))
                .field("postal_code", ip.get("postal_code"))
                .field("lnglat", lnglat)
                .field("metro_code", ip.get("metro_code"))
                .field("area_code", ip.get("area_code"))
                .field("timezone", ip.get("timezone"))
                .field("client_id", client_id)
                .field("user_id", user_id)
                .field("date_time_string", date_time.get("date_time"))
                .field("timestamp", date_time.get("timestamp"))
                .field("day", Integer.parseInt(date_time.get("day")))
                .field("date", Integer.parseInt(date_time.get("date")))
                .field("month", Integer.parseInt(date_time.get("month")))
                .field("year", Integer.parseInt(date_time.get("year")))
                .field("hours", Integer.parseInt(date_time.get("hours")))
                .field("minutes", Integer.parseInt(date_time.get("minutes")))
                .field("seconds", Integer.parseInt(date_time.get("seconds")))
                .field("timezone_offset", Integer.parseInt(date_time.get("timezone_offset")))
                .field("method", method)
                .field("endPoint", endpoint)
                .field("protocol_name", protocol_name)
                .field("protocol_version", protocol_version)
                .field("response_code", response_code)
                .field("content_size", content_size)
                .field("others", others)
                .endObject();
    }

    private HashMap<String, String> getIpFromString(String ip) {

// {city: Paris,
// region_name: Ile-de-France,
// ip_adress: 88.162.200.57,
// timezone: Europe/Paris,
// metro_code: 0,
// area_code: 0,
// country_code: FR,
// postal_code: 75010,
// region_code: A8,
// country_name: France}

        Pattern ip_pattern = Pattern.compile("^(\\S+): (\\S+)," +   // 1-2
                "(\\S+): (\\S+)," +     // 3-4
                "(\\S+): (\\S+)," +     // 5-6
                "(\\S+): (\\S+)," +     // 7-8
                "(\\S+): (\\S+)," +     // 9-10
                "(\\S+): (\\S+)," +     // 11-12
                "(\\S+): (\\S+)," +     // 13-14
                "(\\S+): (\\S+)," +     // 15-16
                "(\\S+): (\\S+)," +     // 17-18
                "(\\S+): (\\S+)" +      // 19-20
                "\\}");
        Matcher mip = ip_pattern.matcher(ip);

        HashMap<String, String> result = new HashMap<>();

        if (!mip.find()) {
            logger.log(Level.ALL, "Cannot parse ip" + ip + " from cassandra");
            throw new RuntimeException("Error parsing ip from cassandra");
        }

        result.put(mip.group(1), mip.group(2));
        result.put(mip.group(3), mip.group(4));
        result.put(mip.group(5), mip.group(6));
        result.put(mip.group(7), mip.group(8));
        result.put(mip.group(9), mip.group(10));
        result.put(mip.group(11), mip.group(12));
        result.put(mip.group(13), mip.group(14));
        result.put(mip.group(15), mip.group(16));
        result.put(mip.group(17), mip.group(18));
        result.put(mip.group(19), mip.group(20));

        return result;
    }

    private HashMap<String, String> getDateFromString(String date) {

// {timezone_offset: -120,
// timestamp: 1410409737000,
// seconds: 57,
// year: 2014,
// minutes: 28,
// hours: 6,
// date: 11,
// date_time: 11/Sep/2014:06:28:57 +0200,
// day: 4,
// month: 9}

        Pattern date_pattern = Pattern.compile("^(\\S+): (\\S+)," +   // 1-2
                "(\\S+): (\\S+)," +     // 3-4
                "(\\S+): (\\S+)," +     // 5-6
                "(\\S+): (\\S+)," +     // 7-8
                "(\\S+): (\\S+)," +     // 9-10
                "(\\S+): (\\S+)," +     // 11-12
                "(\\S+): (\\S+)," +     // 13-14
                "(\\S+): (\\S+)," +     // 15-16
                "(\\S+): (\\S+)," +     // 17-18
                "(\\S+): (\\S+)" +      // 19-20
                "\\}");
        Matcher mdate = date_pattern.matcher(date);

        HashMap<String, String> result = new HashMap<>();

        if (!mdate.find()) {
            logger.log(Level.ALL, "Cannot parse date" + date + " from cassandra");
            throw new RuntimeException("Error parsing date from cassandra");
        }

        result.put(mdate.group(1), mdate.group(2));
        result.put(mdate.group(3), mdate.group(4));
        result.put(mdate.group(5), mdate.group(6));
        result.put(mdate.group(7), mdate.group(8));
        result.put(mdate.group(9), mdate.group(10));
        result.put(mdate.group(11), mdate.group(12));
        result.put(mdate.group(13), mdate.group(14));
        result.put(mdate.group(15), mdate.group(16));
        result.put(mdate.group(17), mdate.group(18));
        result.put(mdate.group(19), mdate.group(20));

        return result;
    }

    private List<Float> getLnglatFromString(String lnglat) {

        Pattern lnglat_pattern = Pattern.compile("^(\\S+),(\\S+)$");
        Matcher mlnglat = lnglat_pattern.matcher(lnglat);

        List<Float  > result = new LinkedList<>();

        if (!mlnglat.find()) {
            logger.log(Level.ALL, "Cannot parse lnglat" + lnglat + " from cassandra");
            throw new RuntimeException("Error parsing lnglat from cassandra");
        }

        this.lnglat.add(Float.parseFloat(mlnglat.group(1)));
        this.lnglat.add(Float.parseFloat(mlnglat.group(2)));

        return result;
    }

}
