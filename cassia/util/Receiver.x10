package cassia.util;

public interface Receiver[T] {

    /**
     * Add the specified value to a temporary storage.
     *
     * @param value a value of T.
     */
    public def receive(value: T): void;

    /**
     * Store the all saved values to the main storage of ReceiverHolder[T]
     */
    public def close(): void;
}
