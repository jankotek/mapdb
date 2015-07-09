package doc;

import org.mapdb10.DBMaker;
import org.mapdb10.Serializer;

import java.util.Map;


public class htreemap_segmented {

    public static void main(String[] args) {
        //a
        Map<String, byte[]> map = DBMaker
               .hashMapSegmentedMemory()
               .keySerializer(Serializer.STRING)
               .valueSerializer(Serializer.BYTE_ARRAY)
               .make();
        //z
    }
}
