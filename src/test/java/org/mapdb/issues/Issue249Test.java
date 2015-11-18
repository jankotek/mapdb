package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TxMaker;

import java.io.Serializable;
import java.util.Map;


public class Issue249Test {

    @Test
    public void main() {
        TxMaker txMaker = DBMaker.memoryDB().closeOnJvmShutdown()
                .makeTxMaker();
        DB db = txMaker.makeTx();

        UploadInfo x = new UploadInfo();
        x.setId(1L);
        x.setTitle("nameXXX");

        Map<Long, UploadInfo> map = db.treeMap(UploadInfo.class.getName());
        map.put(x.getId(), x);

        db = commit(db);
        db = rollback(db);

        DB db2 = txMaker.makeTx();
        Map<Long, UploadInfo> map2 = db2.treeMap(UploadInfo.class.getName());
        map2.get(x.getId());

        txMaker.close();
    }

    private static DB commit(DB db) {
        if (db != null && !db.isClosed())
            db.commit();
        // db = null;
        return db;
    }

    private static DB rollback(DB db) {
        if (db != null && !db.isClosed()) {
            try {
                db.rollback();
            } catch (Exception e) {
            }
        }
        // db = null;
        return db;
    }

    @SuppressWarnings("serial")
    public static class UploadInfo implements Serializable {

        private Long id;
        private String slug;
        private String zipCode;
        private String www;
        private String text;
        private String title;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getSlug() {
            return slug;
        }

        public void setSlug(String slug) {
            this.slug = slug;
        }

        public String getZipCode() {
            return zipCode;
        }

        public void setZipCode(String zipCode) {
            this.zipCode = zipCode;
        }

        public String getWww() {
            return www;
        }

        public void setWww(String www) {
            this.www = www;
        }

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

    }

}