package samples.each;
import cassia.dist.DistCol;
import x10.util.concurrent.Lock;
import x10.xrx.Runtime;

class Evaluate {

    public static def main(args: Rail[String]): void {
        val object: Evaluate;
        val number = args(0);
        while (true) {
            if (number.equals("0")) {
                object = new EvaluateEach();
                break;
            }
            if (number.equals("1")) {
                object = new EvaluateFor();
                break;
            }
            if (number.equals("2")) {
                object = new EvaluateClosure();
                break;
            }
            object = null;
            break;
        }
        object.run();
    }

    def run(): void {
        val distCol = new DistCol[Long]();
        for (var i: Long = 0; i < 1000000; i++) {
            distCol.add(i);
        }
        val begin = System.currentTimeMillis();
        evaluate(distCol);
        val end = System.currentTimeMillis();
        Console.OUT.println(getName() + " " + (end - begin) + "ms");
    }

    def getName(): String {
        return "nothing";
    }

    def evaluate(distCol: DistCol[Long]): void {
        // do nothing
    }

    def getNthreads(): Long {
        // use only one thread
        return 1;
    }


    static class EvaluateEach extends Evaluate {

        def getName(): String {
            return "each";
        }

        def evaluate(distCol: DistCol[Long]): void {
            val sum = new Rail[Long](1);
            sum(0) = 0;
            val lock = new Lock();
            distCol.each(getNthreads(), (values: Iterable[Long]) => {
                var localSum: Long = 0;
                for (value in values) {
                    localSum += value;
                }
                lock.lock();
                sum(0) += localSum;
                lock.unlock();
            });
        }
    }


    static class EvaluateFor extends Evaluate {

        // override
        def getName(): String {
            return "for";
        }

        // override
        def evaluate(distCol: DistCol[Long]): void {
            val sum = new Rail[Long](1);
            sum(0) = 0;
            val lock = new Lock();
            finish {
                for (var i: Long = 0; i < getNthreads(); i++) {
                    async {
                        var localSum: Long = 0;
                        for (value in distCol) {
                            localSum += value;
                        }
                        lock.lock();
                        sum(0) += localSum;
                        lock.unlock();
                    }
                }
            }
        }
    }


    static class EvaluateClosure extends Evaluate {

        // override
        def getName(): String {
            return "closure";
        }

        // override
        def evaluate(distCol: DistCol[Long]): void {
            val sum = new Rail[Long](1);
            sum(0) = 0;
            val lock = new Lock();
            each(() => {
                var localSum: Long = 0;
                for (value in distCol) {
                    localSum += value;
                }
                lock.lock();
                sum(0) += localSum;
                lock.unlock();
            });
        }

        def each(closure: ()=>void) {
            finish {
                for (var i: Long = 0; i < getNthreads(); i++) {
                    async {
                        closure();
                    }
                }
            }
        }
    }
}
