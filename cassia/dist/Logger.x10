package cassia.dist;


import x10.compiler.Inline;
import x10.util.StringBuilder;


/**
 * This class is prepared for internal use.
 * This class is used for collecting the elapsed time of various operations.
 */
final class Logger {
    public var numMoved: Long = 0;
    public var moveTime: Long = 0;
    public var syncTime: Long = 0;
    public var beforeSyncTime: Long = 0;
    public var requestTime: Long = 0;
    public var analyzeTime: Long = 0;
    public var removeTime: Long = 0;
    public var deleteTime: Long = 0;
    public var syncDeleteTime: Long = 0;
    public var distDeleteTime: Long = 0;
    public var relocateTime: Long = 0;
    public var serializeTime: Long = 0;
    public var communicateTime: Long = 0;
    public var deserializeTime: Long = 0;
    public var addTime: Long = 0;
    public var afterSyncTime: Long = 0;
    public var railCopyTime: Long = 0;
    public var distCommTime: Long = 0;
    public var distRestTime: Long = 0;
    public var distAllTime: Long = 0;
    public var syncCommTime: Long = 0;
    public var putTime: Long = 0;
    public var syncPutTime: Long = 0;
    public var distPutTime: Long = 0;
    public var otherTime: Long = 0;
    public var numBytes: Long = 0;

    public final def headerString(): String {
        val stringBuilder = new StringBuilder();
        stringBuilder
            .add("moved\t")
            .add("total\t")
            .add("setup\t")
            .add("ser\t")
            .add("synCom\t")
            .add("dser\t")
            .add("dist\t")
            .add("nBytes");
        return stringBuilder.result();
    }

    public final def resultString(): String {
        val stringBuilder = new StringBuilder();
        stringBuilder
            .add(this.numMoved + "\t")
            .add(this.moveTime + "\t")
            .add(this.requestTime + "\t")
            .add(this.serializeTime + "\t")
            .add(this.syncCommTime + "\t")
            .add(this.deserializeTime + "\t")
            .add(this.distAllTime + "\t")
            .add(this.numBytes);
        return stringBuilder.result();
    }

    @Inline static final def milliTime () = System.currentTimeMillis();

    @Inline static final def nanoTime () = System.nanoTime();

    @Inline static final def time () = milliTime();

    @Inline public final def beginMove() = moveTime -= time();

    @Inline public final def endMove() = moveTime += time();

    @Inline public final def beginSync() = syncTime -= time();

    @Inline public final def endSync() = syncTime += time();

    @Inline public final def beginRequest() = requestTime -= time();

    @Inline public final def endRequest() = requestTime += time();

    @Inline public final def beginRemove() = removeTime -= time();

    @Inline public final def endRemove() = removeTime += time();

    @Inline public final def beginDelete() = deleteTime -= time();

    @Inline public final def endDelete() = deleteTime += time();

    @Inline public final def beginSyncDelete() = syncDeleteTime -= time();

    @Inline public final def endSyncDelete() = syncDeleteTime += time();

    @Inline public final def beginDistDelete() = distDeleteTime -= time();

    @Inline public final def endDistDelete() = distDeleteTime += time();

    @Inline public final def beginRelocate() = relocateTime -= time();

    @Inline public final def endRelocate() = relocateTime += time();

    @Inline public final def beginSerialize() = serializeTime -= time();

    @Inline public final def endSerialize() = serializeTime += time();

    @Inline public final def beginCommunicate() = communicateTime -= time();

    @Inline public final def endCommunicate() = communicateTime += time();

    @Inline public final def beginDeserialize() = deserializeTime -= time();

    @Inline public final def endDeserialize() = deserializeTime += time();

    @Inline public final def beginAdd() = addTime -= time();

    @Inline public final def endAdd() = addTime += time();

    @Inline public final def beginAfterSync() = afterSyncTime -= time();

    @Inline public final def endAfterSync() = afterSyncTime += time();

    @Inline public final def beginRailCopy() = railCopyTime -= time();

    @Inline public final def endRailCopy() = railCopyTime += time();

    @Inline public final def beginDistComm() = distCommTime -= time();

    @Inline public final def endDistComm() = distCommTime += time();

    @Inline public final def beginDistRest() = distRestTime -= time();

    @Inline public final def endDistRest() = distRestTime += time();

    @Inline public final def beginSyncComm() = syncCommTime -= time();

    @Inline public final def endSyncComm() = syncCommTime += time();

    @Inline public final def beginPut() = putTime -= time();

    @Inline public final def endPut() = putTime += time();

    @Inline public final def beginSyncPut() = syncPutTime -= time();

    @Inline public final def endSyncPut() = syncPutTime += time();

    @Inline public final def beginDistPut() = distPutTime -= time();

    @Inline public final def endDistPut() = distPutTime += time();

    @Inline public final def beginOther() = otherTime -= time();

    @Inline public final def endOther() = otherTime += time();

    @Inline public final def beginDistAll() = distAllTime -= time();

    @Inline public final def endDistAll() = distAllTime += time();
}
