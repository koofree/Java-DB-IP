package in.ankushs.dbip.importer;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.net.InetAddresses;
import in.ankushs.dbip.model.GeoAttributes;
import in.ankushs.dbip.model.GeoAttributesImpl;
import in.ankushs.dbip.parser.CsvParser;
import in.ankushs.dbip.parser.CsvParserImpl;
import in.ankushs.dbip.repository.DbIpRepository;
import in.ankushs.dbip.utils.CountryResolver;
import in.ankushs.dbip.utils.GzipUtils;
import in.ankushs.dbip.utils.PreConditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

/**
 * Singleton class responsible for loading the entire file into the JVM.
 *
 * @author Ankush Sharma
 */
public final class ResourceImporter {

    private static final Logger logger = LoggerFactory.getLogger(ResourceImporter.class);
    private static ResourceImporter instance = null;

    private final CsvParser csvParser = CsvParserImpl.getInstance();
    private final DbIpRepository repository;

    private Interner<String> interner = Interners.newWeakInterner();

    private ResourceImporter(DbIpRepository repository) {
        this.repository = repository;
    }

    public static ResourceImporter getInstance(DbIpRepository repository) {
        if (instance == null) {
            return new ResourceImporter(repository);
        }
        return instance;
    }

    /**
     * Loads the file into JVM,reading line by line.
     * Also transforms each line into a GeoEntity object, and save the object into the
     * repository.
     *
     * @param file The dbip-city-latest.csv.gz file as a File object.
     */
    public void load(final File file) {

        try {
            PreConditions.checkExpression(!GzipUtils.isGzipped(file), "Not a  gzip file");
        } catch (final IOException ex) {
            logger.error("", ex);
        }

        try (final InputStream fis = new FileInputStream(file);
             final InputStream gis = new GZIPInputStream(fis);
             final Reader decorator = new InputStreamReader(gis, StandardCharsets.UTF_8);
             final BufferedReader reader = new BufferedReader(decorator);
        ) {
            logger.debug("Reading dbip data from {}", file.getName());
            AtomicInteger i = new AtomicInteger(0);
            reader.lines().parallel().forEach(l -> {
                final String[] array = csvParser.parseRecord(l);
                final GeoAttributes geoAttributes = new GeoAttributesImpl
                        .Builder()
                        .withStartInetAddress(InetAddresses.forString(array[0]))
                        .withEndInetAddress(InetAddresses.forString(array[1]))
                        .withProvince(interner.intern(array[2]))
                        .withCountryCode(array[3])
                        .withCountry(CountryResolver.resolveToFullName(array[3]))
                        .withCity(interner.intern(array[4]))
                        .build();
                repository.save(geoAttributes);
                int now = i.incrementAndGet();
                if (now % 100000 == 0) {
                    logger.debug("Loaded {} entries", now);
                }
            });


        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
