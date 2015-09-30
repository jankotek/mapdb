package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.Serializable;
import java.util.HashSet;

import static org.junit.Assert.assertTrue;

public class Issue164Test {

    public static class Scenario implements Serializable {
        private static final long serialVersionUID = 1L;
        protected String id = null;
        protected String brief = null;
        protected String headNodeId = null;
        protected HashSet nodeIdSet = null;
        public Scenario() {
            id = Long.toHexString(System.nanoTime());
            brief = null;
            headNodeId = null;
            nodeIdSet = null;
        }
        public void setId(String arg_id) {
            synchronized(this) {
                id = arg_id;
            }
        }
        public String getId() {
            synchronized(this) {
                return id;
            }
        }
        public void setBrief(String arg_brief) {
            synchronized(this) {
                brief = arg_brief;
            }
        }
        public String getBrief() {
            synchronized(this) {
                return brief;
            }
        }
        public String getHeadNodeId() {
            synchronized(this) {
                return headNodeId;
            }
        }
        public void setHeadNodeId(String arg_header_node_id) {
            synchronized(this) {
                headNodeId = arg_header_node_id;
                if (!nodeIdSet.contains(arg_header_node_id))
                    nodeIdSet.add(arg_header_node_id);
            }
        }
        public void addConversationNodeId(String arg_conversation_node_id) throws Exception {
            synchronized(this) {
                if (headNodeId == null) {
                    headNodeId = arg_conversation_node_id;
                    nodeIdSet.add(arg_conversation_node_id);
                }
                else
                    throw new Exception();
            } // of synchronized(this)
        }
        public void removeConversationNodeId(String arg_conversation_node_id) {
            synchronized(this) {
                if (headNodeId != null) {
                    nodeIdSet.remove(arg_conversation_node_id);
                    if (arg_conversation_node_id.equals(headNodeId))
                        headNodeId = null; // the set is empty now
                }
            } // of synchronized(this)
        }
    }


    @Test
    public void main() {
        int rc = 0;
        BTreeMap map=null;
        try {
            DB db = DBMaker.memoryDB()
                    .closeOnJvmShutdown()
                    .make();
// the following test shows that the db is opened if it always exists
            map = db.treeMap("test");
            if (!map.containsKey("t1")) {
                map.put("t1", new Scenario());
                db.commit();
            }
            rc = 1;
        } catch(Exception ex) {
            rc = -1;
        }
        assertTrue(map.get("t1")!=null);
        assertTrue(rc > 0);

    }
}