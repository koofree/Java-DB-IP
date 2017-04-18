package in.ankushs.dbip

import com.google.common.net.InetAddresses
import in.ankushs.dbip.api.GeoEntity
import in.ankushs.dbip.lookup.GeoEntityLookupServiceImpl
import in.ankushs.dbip.repository.SmallMapDBDbIpRepositoryImpl
import spock.lang.Specification

/**
 * Created by Swizzle on 2017-04-18.
 */
class DbIpClientSpecForMapDB extends Specification {
    def service

    def setup() {
        service = new GeoEntityLookupServiceImpl(
                new SmallMapDBDbIpRepositoryImpl(new File("/Users/Ankush/Downloads/dbip.db")))
    }

    def "Pass a valid Ip.All should work fine"() {
        when: "Call the client"
        def ip = "216.159.232.248"

        GeoEntity geoEntity = service.lookup(InetAddresses.forString(ip))
        then: "Should return some info.No exception thrown"
        geoEntity.city == 'Columbus'
        geoEntity.country == 'United States'
        geoEntity.province == 'Ohio'

    }
}
