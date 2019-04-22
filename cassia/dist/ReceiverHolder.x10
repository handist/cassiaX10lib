package cassia.dist;
import cassia.util.Receiver;

public interface ReceiverHolder[T] {

    public def getReceiver(): Receiver[T];
}
