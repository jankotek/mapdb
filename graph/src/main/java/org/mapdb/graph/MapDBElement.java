package org.mapdb.graph;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.ElementHelper;

import java.io.Serializable;
import java.util.Set;

public abstract class MapDBElement implements Element, Serializable {

    protected final Object id;
    protected final Long uid;
    protected final MapDBGraph graph;

    protected MapDBElement(Object id, Long uid, MapDBGraph graph) {
        this.id = id;
        this.uid = uid;
        this.graph = graph;
    }

    @Override
    public <T> T getProperty(String key) {
        return (T) graph.getProperty(uid, key);
    }

    @Override
    public Set<String> getPropertyKeys() {
        return graph.getProperties(uid).keySet();
    }

    @Override
    public void setProperty(String key, Object value) {
        ElementHelper.validateProperty(this, key, value);
        graph.setProperty(uid, key, value);
    }

    @Override
    public <T> T removeProperty(String key) {
        return (T) graph.removeProperty(uid, key);
    }

    @Override
    public abstract void remove();

    @Override
    public Object getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        return ElementHelper.areEqual(this, object);
    }

}
