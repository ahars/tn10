
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

    private String ip;
    private String countryCode;
    private String countryName;
    private String regionCode;
    private String regionName;
    private String city;
    private String postalCode;
    private List<Float> lnglat;
    private float latitude;
    private float longitude;
    private int metroCode;
    private int areaCode;
    private String timezone;

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

    public String getCountryCode() { return countryCode; }

    public String getCountryName() { return countryName; }

    public String getRegionCode() { return regionCode; }

    public String getRegionName() { return regionName; }

    public String getCity() { return city; }

    public String getPostalCode() {
        return postalCode;
    }

    public List<Float> getLnglat() { return lnglat; }

    public float getLatitude() {
        return latitude;
    }

    public float getLongitude() {
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

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public void setCountryName(String countryName) {
        this.countryName = countryName;
    }

    public void setRegionCode(String regionCode) { this.regionCode = regionCode; }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setPostalCode(String postalCode) {
        this.postalCode = postalCode;
    }

    public void setLngLat(List<Float> lnglat) { this.lnglat = lnglat; }

    public void setLatitude(float latitude) {
        this.latitude = latitude;
    }

    public void setLongitude(float longitude) {
        this.longitude = longitude;
    }

    public void setMetroCode(int metroCode) {
        this.metroCode = metroCode;
    }

    public void setAreaCode(int areaCode) {
        this.areaCode = areaCode;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public String[] locationIpToIndexString() {
        return new String[]{countryCode, countryName, regionCode, regionName, city, postalCode, String.valueOf(latitude),
                String.valueOf(longitude), String.valueOf(metroCode), String.valueOf(areaCode), timezone};
    }
}
