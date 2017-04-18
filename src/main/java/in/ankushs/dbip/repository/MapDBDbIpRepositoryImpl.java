package in.ankushs.dbip.repository;

import in.ankushs.dbip.api.GeoEntity;
import in.ankushs.dbip.repository.mapdb.SerializerGeoEntity;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.serializer.GroupSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigInteger;
import java.util.Map;

/**
 * Created by Swizzle on 2017-04-12.
 */
public class MapDBDbIpRepositoryImpl extends AbstractDbIpRepository implements DbIpRepository, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MapDBDbIpRepositoryImpl.class);

    private final DB db;
    private final BTreeMap<Integer, GeoEntity> IPV4_REPOSITORY;
    private final BTreeMap<BigInteger, GeoEntity> IPV6_REPOSITORY;

    private final static SerializerGeoEntity serializerGeoEntity = new SerializerGeoEntity();

    public MapDBDbIpRepositoryImpl(File dbFile) {
        db = DBMaker.fileDB(dbFile)
                .fileMmapEnable()
                .fileMmapPreclearDisable()
                .fileChannelEnable()
                .allocateStartSize(512 * 1024 * 1024L)
                .allocateIncrement(64 * 1024 * 1024L)
                .make();
        IPV4_REPOSITORY = db.treeMap("IPV4_REPOSITORY", GroupSerializer.INTEGER, serializerGeoEntity).createOrOpen();
        IPV6_REPOSITORY = db.treeMap("IPV6_REPOSITORY", GroupSerializer.BIG_INTEGER, serializerGeoEntity).createOrOpen();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                MapDBDbIpRepositoryImpl.this.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }));
    }

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

    @Override
    public void close() throws Exception {
        if (db != null && !db.isClosed()) {
            db.commit();
            db.close();
        }
    }
}
