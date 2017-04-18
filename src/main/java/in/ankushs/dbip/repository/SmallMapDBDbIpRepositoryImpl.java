package in.ankushs.dbip.repository;

import in.ankushs.dbip.api.GeoEntity;
import org.eclipse.collections.impl.tuple.ImmutableEntry;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.serializer.GroupSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.*;

/**
 * Created by Swizzle on 2017-04-12.
 */
public class SmallMapDBDbIpRepositoryImpl extends AbstractDbIpRepository implements DbIpRepository, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SmallMapDBDbIpRepositoryImpl.class);

    private final DB db;
    private final BTreeMap<Integer, Long> IPV4_REPOSITORY;
    private final BTreeMap<BigInteger, Long> IPV6_REPOSITORY;
    private final HTreeMap<Integer, String> NAME_REPOSITORY;
    private final HTreeMap<String, Integer> INVERTED_NAME_REPOSITORY;

    public SmallMapDBDbIpRepositoryImpl(File dbFile) {
        db = DBMaker.fileDB(dbFile)
                .fileMmapEnable()
                .fileMmapPreclearDisable()
                .fileChannelEnable()
                .make();
        IPV4_REPOSITORY = db.treeMap("IPV4_REPOSITORY", GroupSerializer.INTEGER_DELTA, GroupSerializer.LONG_PACKED).createOrOpen();
        IPV6_REPOSITORY = db.treeMap("IPV6_REPOSITORY", GroupSerializer.BIG_INTEGER, GroupSerializer.LONG_PACKED).createOrOpen();
        NAME_REPOSITORY = db.hashMap("NAME_REPOSITORY", GroupSerializer.INTEGER_DELTA, GroupSerializer.STRING_DELTA2).createOrOpen();
        INVERTED_NAME_REPOSITORY = db.hashMap("INVERTED_NAME_REPOSITORY", GroupSerializer.STRING_DELTA2, GroupSerializer.INTEGER_DELTA).createOrOpen();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }));
    }

    @Override
    Map.Entry<Integer, GeoEntity> IPV4FloorEntry(Integer i) {
        Map.Entry<Integer, Long> e = IPV4_REPOSITORY.floorEntry(i);
        return new ImmutableEntry<>(e.getKey(), convert(e.getValue()));
    }

    @Override
    Map.Entry<BigInteger, GeoEntity> IPV6FloorEntry(BigInteger i) {
        Map.Entry<BigInteger, Long> e = IPV6_REPOSITORY.floorEntry(i);
        return new ImmutableEntry<>(e.getKey(), convert(e.getValue()));
    }

    @Override
    void IPV4Put(Integer i, GeoEntity e) {
        IPV4_REPOSITORY.put(i, convert(e));
    }

    @Override
    void IPV6Put(BigInteger i, GeoEntity e) {
        IPV6_REPOSITORY.put(i, convert(e));
    }

    private static final long t1 = 15000;
    private static final long t2 = multiplyExact(t1, t1);
    private static final long t3 = multiplyExact(t1, t2);

    private final static AtomicInteger index = new AtomicInteger(0);

    private Long convert(GeoEntity geoEntity) {
        Integer city = getOrCreate(geoEntity.getCity());
        Integer country = getOrCreate(geoEntity.getCountry());
        Integer countryCode = getOrCreate(geoEntity.getCountryCode());
        Integer province = getOrCreate(geoEntity.getProvince());

        return addExact(addExact(addExact(city, multiplyExact(country, t1)), multiplyExact(countryCode, t2)), multiplyExact(province, t3));
    }

    private Integer getOrCreate(String key) {
        int result = INVERTED_NAME_REPOSITORY.getOrDefault(key, 0);
        if (result == 0) {
            result = index.incrementAndGet();
            INVERTED_NAME_REPOSITORY.put(key, result);
            NAME_REPOSITORY.put(result, key);
        }
        return result;
    }

    private GeoEntity convert(Long result) {
        int province = toIntExact(Math.floorDiv(result, t3));
        result = Math.floorMod(result, t3);
        int countryCode = toIntExact(Math.floorDiv(result, t2));
        result = Math.floorMod(result, t2);
        int country = toIntExact(Math.floorDiv(result, t1));
        result = Math.floorMod(result, t1);
        int city = toIntExact(result);
        return new GeoEntity.Builder()
                .withCity(NAME_REPOSITORY.get(city))
                .withCountry(NAME_REPOSITORY.get(country))
                .withCountryCode(NAME_REPOSITORY.get(countryCode))
                .withProvince(NAME_REPOSITORY.get(province))
                .build();
    }

    @Override
    public void close() throws Exception {
        if (db != null && !db.isClosed()) {
            db.commit();
            db.close();
        }
    }
}
