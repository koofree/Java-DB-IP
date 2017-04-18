package in.ankushs.dbip.repository.mapdb;

import in.ankushs.dbip.api.GeoEntity;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializerObjectArray;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Created by Swizzle on 2017-04-12.
 */
public class SerializerGeoEntity extends GroupSerializerObjectArray<GeoEntity> {

    @Override
    public void serialize(@NotNull DataOutput2 out, @NotNull GeoEntity value) throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(value);
        oos.close();
    }

    @Override
    public GeoEntity deserialize(@NotNull DataInput2 input, int available) throws IOException {
        ObjectInputStream ois = new ObjectInputStream(new DataInput2.DataInputToStream(input));
        try {
            Object o = ois.readObject();
            ois.close();
            return (GeoEntity) o;
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
