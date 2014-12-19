package formatLog;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.timeZone;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public class LocationIp {

    private static final Logger logger = Logger.getLogger("LocationIp");

    private static final String PATH = "/home/ippon/github/sparktacus/src/data/";
    private File file = new File(PATH + "/GeoLiteCity.dat");

    private String ipAdress;
    private String countryCode;
    private String countryName;
    private String regionCode;
    private String regionName;
    private String city;
    private String postalCode;
    private int metroCode;
    private int areaCode;
    private String timezone;
    private List<Float> lnglat = null;

    public LocationIp(String ipAdress) {

        this.ipAdress = ipAdress;

        Location l = null;
        try {
            LookupService cl = new LookupService(file, LookupService.GEOIP_MEMORY_CACHE );
            l = cl.getLocation(ipAdress);
            cl.close();
        }
        catch (IOException e) {
            logger.warning("error LocationIp");
        }

        this.countryCode = l.countryCode;
        this.countryName = l.countryName;
        this.regionCode = l.region;
        this.regionName = com.maxmind.geoip.regionName.regionNameByCode(l.countryCode, l.region);
        this.city = l.city;
        this.postalCode = l.postalCode;
        this.metroCode = l.metro_code;
        this.areaCode = l.area_code;
        this.timezone = timeZone.timeZoneByCountryAndRegion(l.countryCode, l.region);

        this.lnglat = new LinkedList<Float>();
        this.lnglat.add(l.longitude);
        this.lnglat.add(l.latitude);
    }

    @Override
    public String toString() {
        return "LocationIp{" +
                ", ipAdress='" + ipAdress + '\'' +
                ", countryCode='" + countryCode + '\'' +
                ", countryName='" + countryName + '\'' +
                ", regionCode='" + regionCode + '\'' +
                ", regionName='" + regionName + '\'' +
                ", city='" + city + '\'' +
                ", postalCode='" + postalCode + '\'' +
                ", metroCode=" + metroCode +
                ", areaCode=" + areaCode +
                ", timezone='" + timezone + '\'' +
                ", lnglat=" + lnglat +
                '}';
    }
}

