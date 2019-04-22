/* 要素を列挙できるタイプの Distribution, IterableDistribution などに改名すべ?　*/
/* 取り敢えずは、Long 専用で。本来 Range[K] { K <: Comparable} 対応にすべきかな？ */
package cassia.dist;
import x10.util.List;
import x10.util.Pair;

interface RangedDistribution[R] {
    public def placeRanges(range: R): List[Pair[Place, R]];

}
