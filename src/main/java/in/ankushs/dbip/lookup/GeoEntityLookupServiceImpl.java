package in.ankushs.dbip.lookup;

import in.ankushs.dbip.api.GeoEntity;
import in.ankushs.dbip.repository.DbIpRepository;
import in.ankushs.dbip.utils.PreConditions;

import java.net.InetAddress;

/**
 * Singleton class that resolves ip to Location info.
 *
 * @author Ankush Sharma
 */
public class GeoEntityLookupServiceImpl implements GeoEntityLookupService {

    private static final String UNKNOWN = "Unknown";

    private final DbIpRepository repository;

    public GeoEntityLookupServiceImpl(DbIpRepository repository) {
        this.repository = repository;
    }

    @Override
    public GeoEntity lookup(final InetAddress inetAddress) {
        PreConditions.checkNull(inetAddress, "inetAddress cannot be null ");
        GeoEntity geoEntity = repository.get(inetAddress);
        if (geoEntity == null) {
            geoEntity = new GeoEntity
                    .Builder()
                    .withCity(UNKNOWN)
                    .withCountry(UNKNOWN)
                    .withCountryCode(UNKNOWN)
                    .withProvince(UNKNOWN)
                    .build();
        }
        return geoEntity;
    }
}
