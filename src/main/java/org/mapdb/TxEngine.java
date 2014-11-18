package org.mapdb;

public class TxEngine extends EngineWrapper{

    protected TxEngine(Engine engine) {
        super(engine);
    }

    public static Engine createSnapshotFor(Engine engine) {
        return null;
    }
}
