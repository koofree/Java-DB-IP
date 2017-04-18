package in.ankushs.dbip.repository;

import com.google.common.net.InetAddresses;
import in.ankushs.dbip.api.GeoEntity;
import in.ankushs.dbip.model.GeoAttributes;
import in.ankushs.dbip.utils.IPUtils;
import in.ankushs.dbip.utils.PreConditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Map;

/**
 * Created by Swizzle on 2017-04-12.
 */
abstract class AbstractDbIpRepository implements DbIpRepository {
    private static final Logger logger = LoggerFactory.getLogger(AbstractDbIpRepository.class);

    /**
     * Lookup GeoEntity for an InetAddress
     *
     * @param inetAddress The InetAddress to be resolved.
     * @return A GeoEntity for an InetAddress
     */
    @Override
    public GeoEntity get(final InetAddress inetAddress) {
        PreConditions.checkNull(inetAddress, "inetAddress must not be null");
        if (inetAddress instanceof Inet4Address) {
            final Integer startIpNum = InetAddresses.coerceToInteger(inetAddress);

            Map.Entry<Integer, GeoEntity> entry = IPV4FloorEntry(startIpNum);
            return entry == null ? null : entry.getValue();
        } else {
            final BigInteger startIpBigInt = IPUtils.ipv6ToBigInteger(inetAddress);

            Map.Entry<BigInteger, GeoEntity> entry = IPV6FloorEntry(startIpBigInt);
            return entry == null ? null : entry.getValue();
        }
    }

    /**
     * Save GeoEntity for an InetAddress
     *
     * @param geoAttributes The attributes to be saved . Contains the attributes that will be needed
     *                      as key and value to be put inside the TreeMap.
     */
    @Override
    public void save(final GeoAttributes geoAttributes) {
        PreConditions.checkNull(geoAttributes, "geoAttributes must not be null");
        final InetAddress startInetAddress = geoAttributes.getStartInetAddress();
        final InetAddress endInetAddress = geoAttributes.getEndInetAddress();
        final GeoEntity geoEntity = geoAttributes.getGeoEntity();

        if (startInetAddress instanceof Inet6Address
                && endInetAddress instanceof Inet6Address) {
            final BigInteger startIpBigInt = IPUtils.ipv6ToBigInteger(startInetAddress);
            IPV6Put(startIpBigInt, geoEntity);
        } else if (startInetAddress instanceof Inet4Address
                && endInetAddress instanceof Inet4Address) {
            final Integer startIpNum = InetAddresses.coerceToInteger(startInetAddress);
            IPV4Put(startIpNum, geoEntity);
        } else {
            //Well, this case should never happen. Maybe I'll throw in an exception later.
            logger.warn("This shouldn't ever happen");
        }
    }

    abstract Map.Entry<Integer, GeoEntity> IPV4FloorEntry(Integer i);

    abstract Map.Entry<BigInteger, GeoEntity> IPV6FloorEntry(BigInteger i);

    abstract void IPV4Put(Integer i, GeoEntity e);

    abstract void IPV6Put(BigInteger i, GeoEntity e);
}
