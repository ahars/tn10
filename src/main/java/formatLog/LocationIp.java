package formatLog;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.timeZone;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class LocationIp {

    private static final String PATH = "C:\\Users\\IPPON_2\\Desktop\\tn10\\sparky\\src\\data\\";
    private File file = new File(PATH + "GeoLiteCity.dat");

    private String ip = null;
    private String countryCode = null;
    private String countryName = null;
    private String regionCode = null;
    private String regionName = null;
    private String city = null;
    private String postalCode = null;
    private List<Float> lnglat = null;
    private float latitude = 0;
    private float longitude = 0;
    private int metroCode = 0;
    private int areaCode = 0;
    private String timezone = null;

    public LocationIp(String ip) {
        this.ip = ip;

        try {
            LookupService cl = new LookupService(file, LookupService.GEOIP_MEMORY_CACHE );
            Location l = cl.getLocation(ip);

            this.countryCode = l.countryCode;
            this.countryName = l.countryName;
            this.regionCode = l.region;
            this.regionName = com.maxmind.geoip.regionName.regionNameByCode(l.countryCode, l.region);
            this.city = l.city;
            this.postalCode = l.postalCode;
            this.lnglat = new LinkedList<>();
            this.lnglat.add(l.longitude);
            this.lnglat.add(l.latitude);
            this.latitude = l.latitude;
            this.longitude = l.longitude;
            this.metroCode = l.metro_code;
            this.areaCode = l.area_code;
            this.timezone = timeZone.timeZoneByCountryAndRegion(l.countryCode, l.region);

            cl.close();
        }
        catch (IOException e) {
            System.out.println("IO Exception");
        }
    }

    public String getIp() { return ip; }
    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getCountryCode() { return countryCode; }
    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public String getCountryName() { return countryName; }
    public void setCountryName(String countryName) {
        this.countryName = countryName;
    }

    public String getRegionCode() { return regionCode; }
    public void setRegionCode(String regionCode) { this.regionCode = regionCode; }

    public String getRegionName() { return regionName; }
    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public String getCity() { return city; }
    public void setCity(String city) {
        this.city = city;
    }

    public String getPostalCode() {
        return postalCode;
    }
    public void setPostalCode(String postalCode) {
        this.postalCode = postalCode;
    }

    public List<Float> getLnglat() { return lnglat; }
    public void setLngLat(List<Float> lnglat) { this.lnglat = lnglat; }

    public float getLatitude() {
        return latitude;
    }
    public void setLatitude(float latitude) {
        this.latitude = latitude;
    }

    public float getLongitude() {
        return longitude;
    }
    public void setLongitude(float longitude) {
        this.longitude = longitude;
    }

    public int getMetroCode() {
        return metroCode;
    }
    public void setMetroCode(int metroCode) {
        this.metroCode = metroCode;
    }

    public int getAreaCode() {
        return areaCode;
    }
    public void setAreaCode(int areaCode) {
        this.areaCode = areaCode;
    }

    public String getTimezone() {
        return timezone;
    }
    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public String[] locationIpToIndexString() {
        return new String[]{countryCode, countryName, regionCode, regionName, city, postalCode, String.valueOf(latitude),
                String.valueOf(longitude), String.valueOf(metroCode), String.valueOf(areaCode), timezone};
    }
}
