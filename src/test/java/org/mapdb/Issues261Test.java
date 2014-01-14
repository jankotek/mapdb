package org.mapdb;


import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.util.Map;
import java.util.Random;

public class Issues261Test {

    static  class HotelDescriptionTranslation implements Serializable {
        public final Integer hotel_id;
        public final Integer descriptiontype_id;
        public final String description;
        public final String languagecode;

        HotelDescriptionTranslation() {

            this.hotel_id = new Random().nextInt();
            this.descriptiontype_id = new Random().nextInt();
            this.description = UtilsTest.randomString(new Random().nextInt(100));
            this.languagecode = UtilsTest.randomString(new Random().nextInt(100));
        }
    }

    @Test
    public void test(){
        DB db = DBMaker
                //.newTempFileDB()
                .newFileDB(new File("file"))
                .closeOnJvmShutdown()
                .transactionDisable()
                .cacheSize(32768)
                .mmapFileEnablePartial()
                .cacheLRUEnable()
                .fullChunkAllocationEnable()
//                .readOnly()
                .make();

        Map m = db.createTreeMap("MAIN_TREE_MAP")
                .counterEnable()
                .valuesOutsideNodesEnable()
                .makeOrGet();

        for(int i=0;i<1e5;i++){
            m.put(i,new HotelDescriptionTranslation());
        }
        db.close();
    }

}
