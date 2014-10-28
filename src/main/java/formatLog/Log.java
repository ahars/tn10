package formatLog;

import com.datastax.spark.connector.CassandraRow;
import com.datastax.spark.connector.types.TypeConverter;
import org.apache.cassandra.utils.UUIDGen;
import org.elasticsearch.common.xcontent.XContentBuilder;
import scala.collection.JavaConversions;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Log implements Serializable {

    private static final Logger logger = Logger.getLogger("Log");

    private UUID id = null;
    private Map<String, String> ip = null;
    private List<Float> lnglat = null;
    private String client_id = null;
    private String user_id = null;
    private Map<String, String> date_time = null;
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

    public Log(CassandraRow crow) {

        this.id = crow.getUUID("id");
        this.ip = JavaConversions.asJavaMap(crow.getMap("ip", new TypeConverter.StringConverter$(),
                new TypeConverter.StringConverter$()));
        this.lnglat = JavaConversions.asJavaList(crow.getList("lnglat", new TypeConverter.JavaFloatConverter$()));
        this.client_id = crow.getString("client_id");
        this.user_id = crow.getString("user_id");
        this.date_time = JavaConversions.asJavaMap(crow.getMap("date_time", new TypeConverter.StringConverter$(),
                new TypeConverter.StringConverter$()));
        this.method = crow.getString("method");
        this.endpoint = crow.getString("endpoint");
        this.protocol_name = crow.getString("protocol_name");
        this.protocol_version = crow.getString("protocol_version");
        this.response_code = crow.getInt("response_code");
        this.content_size =  crow.getInt("content_size");
        this.others = crow.getString("others");
    }

    public UUID getId() { return id; }
    public void setId() { this.id = UUIDGen.getTimeUUID(); }

    public Map getIp() { return ip; }
    public void setIp(Map<String, String> ip) { this.ip = ip; }

    public List<Float> getLnglat() { return lnglat; }
    public void setLnglat(List<Float> lnglat) { this.lnglat = lnglat; }

    public String getClient_id() { return client_id; }
    public void setClient_id(String client_id) { this.client_id = client_id; }

    public String getUser_id() { return user_id; }
    public void setUser_id(String user_id) { this.user_id = user_id; }

    public Map<String, String> getDate_time() { return date_time; }
    public void setDate_time_string(Map<String, String> date_time) { this.date_time = date_time; }

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
                .field("day", date_time.get("day"))
                .field("date", date_time.get("date"))
                .field("month", date_time.get("month"))
                .field("year", date_time.get("year"))
                .field("hours", date_time.get("hours"))
                .field("minutes", date_time.get("minutes"))
                .field("seconds", date_time.get("seconds"))
                .field("timezone_offset", date_time.get("timezone_offset"))
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
