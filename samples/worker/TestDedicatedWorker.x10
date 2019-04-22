package samples.worker;
import cassia.concurrent.DedicatedWorker;
import x10.util.concurrent.Condition;
import x10.util.StringBuilder;

public class TestDedicatedWorker {

    public static def main(args: Rail[String]): void {
        val object = new TestDedicatedWorker(args);
        object.run();
    }

    val n: Long;

    def this(args: Rail[String]) {
        if (args.size > 0) {
            n = Long.parse(args(0));
        } else {
            n = 10;
        }
    }

    def run(): void {
        val worker = new DedicatedWorker();
        val sum = new Rail[Long](1);
        sum(0) = 0;
        worker.start();
        for (1..n) {
            worker.submit(() => {
                sum(0) = sum(0) + 1;
            });
        }
        val condition = new Condition();
        worker.submit(() => {
            condition.release();
        });
        condition.await();
        worker.stop();
        val sb = new StringBuilder();
        sb.add("sum = " + sum(0) + " (expected = " + n + ")");
        Console.OUT.println(sb);
    }
}
