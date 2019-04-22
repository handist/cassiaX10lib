package cassia.concurrent;
import x10.compiler.Uncounted;
import x10.util.concurrent.AtomicBoolean;
import x10.xrx.Runtime;

/**
 * daemon.
 */
public class Daemon {

	private val terminate: AtomicBoolean = new AtomicBoolean(false);

	public def start(): void {
		@Uncounted async {
            while (loop());
        }
	}

	private def loop(): Boolean {
		if (terminate.get()) {
			return false; // stop loop
		}
		Runtime.probe();
		return true; // continue loop
	}

	public def stop(): void {
		terminate.set(false);
	}
}
