package cassia.util;

public struct Triplets[T1, T2, T3] {
    public val first: T1;
    public val second: T2;
    public val third: T3;
    def this(first: T1, second: T2, third: T3) {
        this.first = first;
	this.second = second;
	this.third = third;
    }
}
