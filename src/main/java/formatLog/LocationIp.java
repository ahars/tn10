package formatLog;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.timeZone;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class LocationIp {

    private static final String PATH = "C:\\Users\\IPPON_2\\Desktop\\tn10\\sparky\\src\\data\\";
    private File file = new File(PATH + "GeoLiteCity.dat");
    //private static final String PATH = "/Users/ahars/sparky/src/data/";
    //private File file = new File(PATH + "/GeoLiteCity.dat");

    private HashMap<String, String> ip_adress = null;
    private List<Float> lnglat = null;

    public LocationIp(String ip_adress) {

        this.ip_adress = new HashMap<>();
        this.ip_adress.put("ip_adress", ip_adress);

        try {
            LookupService cl = new LookupService(file, LookupService.GEOIP_MEMORY_CACHE );
            Location l = cl.getLocation(ip_adress);

            this.ip_adress.put("country_code", l.countryCode);
            this.ip_adress.put("country_name", l.countryName);
            this.ip_adress.put("region_code", l.region);
            this.ip_adress.put("region_name", com.maxmind.geoip.regionName.regionNameByCode(l.countryCode, l.region));
            this.ip_adress.put("city", l.city);
            this.ip_adress.put("postal_code", l.postalCode);
            this.ip_adress.put("metro_code", Integer.toString(l.metro_code));
            this.ip_adress.put("area_code", Integer.toString(l.area_code));
            this.ip_adress.put("timezone", timeZone.timeZoneByCountryAndRegion(l.countryCode, l.region));

            this.lnglat = new LinkedList<Float>();
            this.lnglat.add(l.longitude);
            this.lnglat.add(l.latitude);

            cl.close();
            conversionNullToString();
        }
        catch (IOException e) {
            System.out.println("error LocationIp");
        }
    }

    public HashMap getIp_adress() { return ip_adress; }
    public void setIp_adress(HashMap<String, String> ip_adress) {
        this.ip_adress = ip_adress;
    }

    public List<Float> getLnglat() { return lnglat; }
    public void setLngLat(List<Float> lnglat) { this.lnglat = lnglat; }

    private void conversionNullToString () {
        this.ip_adress.replace("ip_adress", null, "");
        this.ip_adress.replace("country_code", null, "");
        this.ip_adress.replace("country_name", null, "");
        this.ip_adress.replace("region_code", null, "");
        this.ip_adress.replace("region_name", null, "");
        this.ip_adress.replace("city", null, "");
        this.ip_adress.replace("postal_code", null, "");
        this.ip_adress.replace("metro_code", null, "");
        this.ip_adress.replace("area_code", null, "");
        this.ip_adress.replace("timezone", null, "");
    }
}
