package in.ankushs.dbip.repository;

import in.ankushs.dbip.api.GeoEntity;

import java.math.BigInteger;
import java.util.Map;
import java.util.TreeMap;

/**
 * Singletonthat uses a <a href="https://docs.oracle.com/javase/7/docs/api/java/util/TreeMap.html">TreeMap</a>
 * as repository.
 *
 * @author Ankush Sharma
 */
public class JavaMapDbIpRepositoryImpl extends AbstractDbIpRepository implements DbIpRepository {

    private static JavaMapDbIpRepositoryImpl instance = null;

    private JavaMapDbIpRepositoryImpl() {
    }

    public static JavaMapDbIpRepositoryImpl getInstance() {
        if (instance == null) {
            instance = new JavaMapDbIpRepositoryImpl();
        }
        return instance;
    }

    private final static TreeMap<Integer, GeoEntity> IPV4_REPOSITORY = new TreeMap<>();
    private final static TreeMap<BigInteger, GeoEntity> IPV6_REPOSITORY = new TreeMap<>();

    @Override
    Map.Entry<Integer, GeoEntity> IPV4FloorEntry(Integer i) {
        return IPV4_REPOSITORY.floorEntry(i);
    }

    @Override
    Map.Entry<BigInteger, GeoEntity> IPV6FloorEntry(BigInteger i) {
        return IPV6_REPOSITORY.floorEntry(i);
    }

    @Override
    void IPV4Put(Integer i, GeoEntity e) {
        IPV4_REPOSITORY.put(i, e);
    }

    @Override
    void IPV6Put(BigInteger i, GeoEntity e) {
        IPV6_REPOSITORY.put(i, e);
    }
}
