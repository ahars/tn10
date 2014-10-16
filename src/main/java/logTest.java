import java.io.Serializable;
import java.util.logging.Logger;

public class logTest implements Serializable {

    private static final Logger logger = Logger.getLogger("Access");
    private Integer id;
    private String ip;

    public logTest(logTest log) {
        this.id = log.getId();
        this.ip = log.getIp();
    }

    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public String getIp() { return ip; }
    public void setIp(String ip) { this.ip = ip; }

    public logTest(Integer id, String ip) {
        this.id = id;
        this.ip = ip;
    }

    public String toString() {
        return String.format("id = %d, ip = %s", id, ip);
    }
}
