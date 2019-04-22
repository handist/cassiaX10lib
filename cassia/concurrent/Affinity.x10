package cassia.concurrent;
import x10.compiler.Native;
import x10.compiler.NativeCPPInclude;

@NativeCPPInclude("cassia_affinity.h")

public class Affinity {

    public static def bind(cpu: Long): void {
        bind(cpu as Int);
    }

    public static def bind(cpu: Int): void {
        @Native("c++", "Cassia::promote(cpu);") {}
    }

    public static def bind(rail: Rail[Long]): void {
        if (rail.size == 0) {
            return;
        }
        val tmp = new Rail[Int](rail.size, (i: Long) => (rail(i) as Int));
        bind(tmp);
        Unsafe.dealloc(tmp);
    }

    public static def bind(rail: Rail[Int]): void {
        if (rail.size == 0) {
            return;
        }
        @Native("c++", "Cassia::promote(rail);") {}
    }

    public static def bind(range: LongRange): void {
        bind(range as IntRange);
    }

    public static def bind(range: IntRange): void {
        bind(range.min, range.max);
    }

    private static def bind(min: Int, max: Int): void {
        @Native("c++", "Cassia::promote(min, max);") {}
    }
}
