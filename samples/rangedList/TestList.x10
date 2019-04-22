package samples.rangedList;
import x10.util.*;
import cassia.util.*;

public class TestList[T] extends TestAny {
    
    private val list: List[T];

    public def this(messages: ArrayList[String], list: List[T]) {
        super(messages, list);
	this.list = list;
    }

// Interface List[T]

    public def testAddBefore(i: Long, v: T) {
        messages.add("CALL: " + className + "::addBefore " + "(" + i + ", " + v + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    list.addBefore(i, v);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testGetFirst() {
        messages.add("CALL: " + className + "::getFirst " + "()");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.getFirst();
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testGetLast() {
        messages.add("CALL: " + className + "::getLast " + "()");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.getLast();
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testIndexOf(v:T) {
        messages.add("CALL: " + className + "::indexOf " + "(" + v + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.indexOf(v);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testIndexOf(index:Long, v:T) {
        messages.add("CALL: " + className + "::indexOf " + "(" + index + ", " + v + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.indexOf(index, v);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testIndices() {
        messages.add("CALL: " + className + "::indices " + "()");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.indices();
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testIterator() {
        messages.add("CALL: " + className + "::iterator " + "()");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.iterator();
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testIteratorFrom(index: Long) {
        messages.add("CALL: " + className + "::iteratorFrom " + "(" + index + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.iteratorFrom(index);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testLastIndexOf(v: T) {
        messages.add("CALL: " + className + "::lastIndexOf " + "(" + v + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.lastIndexOf(v);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testLastIndexOf(index: Long, v: T) {
        messages.add("CALL: " + className + "::lastIndexOf " + "(" + index + ", " + v + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.lastIndexOf(index, v);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testRemoveAt(i: Long) {
        messages.add("CALL: " + className + "::removeAt " + "(" + i + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.removeAt(i);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testRemoveFirst() {
        messages.add("CALL: " + className + "::removeFirst " + "()");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.removeFirst();
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testRemoveLast() {
        messages.add("CALL: " + className + "::removeLast " + "()");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.removeLast();
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testReverse() {
        messages.add("CALL: " + className + "::reverse " + "()");
	try {
            messages.add("BEFORE: List " + list.toString());
	    list.reverse();
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testSort() {T <: x10.lang.Comparable[T]} {
        messages.add("CALL: " + className + "::sort " + "()");
	try {
            messages.add("BEFORE: List " + list.toString());
	    list.sort();
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testSort(cmp: (T,T)=>Int) {
        messages.add("CALL: " + className + "::sort " + "(" + cmp.toString() + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    list.sort(cmp);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testSubList(fromIndex: Long, toIndex: Long) {
        messages.add("CALL: " + className + "::subList " + "(" + fromIndex + ", " + toIndex + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.subList(fromIndex, toIndex);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

// Interface x10.util.Collection

    public def testAdd(v: T) {
        messages.add("CALL: " + className + "::add " + "(" + v + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.add(v);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//Boolean
    }

    public def testAddAll(c: Container[T]) {
        messages.add("CALL: " + className + "::addAll " + "(" + c + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.addAll(c);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//Boolean
    }

    public def testAddAllWhere(c: Container[T], p: (T)=>Boolean) {
        messages.add("CALL: " + className + "::addAllWhere " + "(" + c + ", " + p + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.addAllWhere(c, p);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//Boolean    
    }

    public def testClear() {
        messages.add("CALL: " + className + "::clear " + "()");
	try {
            messages.add("BEFORE: List " + list.toString());
	    list.clear();
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//void
    }

    public def testClone() {
        messages.add("CALL: " + className + "::clone " + "()");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.clone();
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc);
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//Collection[T]
    }

    public def testRemove(v: T) {
        messages.add("CALL: " + className + "::remove " + "(" + v + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.remove(v);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc);
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//Boolean
    }

    public def testRemoveAll(c: Container[T]) {
        messages.add("CALL: " + className + "::removeAll " + "(" + c + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.removeAll(c);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc);
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//Boolean
    }

    public def testRemoveAllWhere(p: (T)=>Boolean) {
        messages.add("CALL: " + className + "::removeAllWhere " + "(" + p + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.removeAllWhere(p);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc);
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//Boolean
    }

    public def testRetainAll(c: Container[T]) {
        messages.add("CALL: " + className + "::retainAll " + "(" + c + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.retainAll(c);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc);
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//void
    }

// Interface x10.util.Indexed

    public def testOperatorThis(index: Long) {
        messages.add("CALL: " + className + "::operatorThis " + "(" + index + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list(index);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc);
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//T
    }   

// Interface x10.util.Container

/*
    public def testClone() {
        messages.add("CALL: " + className + "::clone " + "()");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.clone();
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc);
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//Container[T]
    }
*/

    public def testContains(y: T) {
        messages.add("CALL: " + className + "::contains " + "(" + y + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.contains(y);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc);
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//Boolean
    }

    public def testContainsAll(c: Container[T]) {
        messages.add("CALL: " + className + "::containsAll " + "(" + c + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.containsAll(c);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc);
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//Boolean
    }

    public def testIsEmpty() {
        messages.add("CALL: " + className + "::isEmpty " + "()");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.isEmpty();
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc);
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//Boolean
    }

    public def testSize() {
        messages.add("CALL: " + className + "::size " + "()");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = list.size();
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc);
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//Long
    }

// Interface x10.lang.Settable

    public def testOperatorThisEqual(i: Long, v: T) {
        messages.add("CALL: " + className + "::operatorThisEqual " + "(" + i + ", " + v + ")");
	try {
            messages.add("BEFORE: List " + list.toString());
	    val rc = (list(i) = v);
	    messages.add("AFTER: List " + list.toString());
	    messages.add("RETURN: " + rc);
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
	//T
    }

// Scenario

    public def scenario(v1: T, v2: T, v3: T) {T <: x10.lang.Comparable[T]} {
        messages.add("START: " + className + " scenario List");
	try {
            messages.add("BEFORE: List " + list.toString());

	    testTypeName();
	    testToString();
	    testHashCode();
	    testEquals(list);
	    testEquals(this);
	    testGetFirst();
	    testGetLast();
	    testIndices();
	    val indices = list.indices();
	    val firstIndex = indices.getFirst();
	    val lastIndex = indices.getLast();
	    val middleIndex = (lastIndex - firstIndex + 1) / 2 + firstIndex;
	    testAddBefore(firstIndex, v1);
	    testAddBefore(firstIndex - 1, v1);
	    testAddBefore(firstIndex + 1, v1);
	    testIndexOf(v2);
	    testIndexOf(middleIndex, v2);
	    testIndexOf(v3);
	    testIndexOf(middleIndex, v3);
	    testIterator();
//	    val testit1 = new TestListIterator(messages, list.iterator());
//	    testit1.scenario(v1, v2);
	    testIteratorFrom(middleIndex);
	    val testit2 = new TestListIterator(messages, list.iteratorFrom(15));
	    testit2.scenario(v1, v2);
	    testLastIndexOf(v2);
	    testLastIndexOf(v3);
	    testLastIndexOf(middleIndex, v2);
	    testLastIndexOf(middleIndex, v3);
	    val indices2 = list.indices();
	    val firstIndex2 = indices2.getFirst();
	    val lastIndex2 = indices2.getLast();
	    testRemoveAt(firstIndex2);
	    testRemoveAt(firstIndex2);
	    testRemoveAt(firstIndex2 + 1);
	    testRemoveAt(firstIndex2 + 1);
	    val indices3 = list.indices();
	    val firstIndex3 = indices.getFirst();
	    val lastIndex3 = indices.getLast();
	    testRemoveAt(lastIndex);
	    testRemoveAt(lastIndex);
	    testRemoveAt(lastIndex - 1);
	    testRemoveAt(lastIndex - 1);
	    testRemoveFirst();
	    testRemoveLast();
	    testReverse();
	    testSort();
	    testSort((x: T, y: T) => 0N);

	    val indices4 = list.indices();
	    val firstIndex4 = indices4.getFirst();
	    val lastIndex4 = indices4.getLast();
	    val middleIndex4 = (lastIndex4 - firstIndex4 + 1) / 2 + firstIndex4;
	    testSubList(firstIndex4, middleIndex4);
	    testSubList(middleIndex4, firstIndex4);
	    testSubList(lastIndex4, middleIndex4);
	    testSubList(middleIndex4, lastIndex4);
	    testSubList(middleIndex4, middleIndex4);
	    testSubList(firstIndex4, lastIndex4);
	    testSubList(-1, lastIndex4);
	    testSubList(firstIndex4, lastIndex4 + 1);

	    // should insert test for subList (RangedListView) here

	    testAdd(v1);

	    val c1 = new ArrayList[T]();
	    c1.add(v2);
	    c1.add(v1);
	    c1.add(v2);
	    c1.add(v1);

	    testAddAll(c1);
	    val indices5 = list.indices();
	    val firstIndex5 = indices5.getFirst();
	    val lastIndex5 = indices5.getLast();
	    testSubList(lastIndex5 - 6, lastIndex5);

	    testAddAllWhere(c1, (T) => true);
	    testAddAllWhere(c1, (T) => false);

	    testClear();
	    testClone();

	    testRemove(v1);
	    testRemoveAll(c1);
	    testRemoveAllWhere((T) => false);
	    testRetainAll(c1);
	    
	    try {
	    val indices6 = list.indices();
	    val firstIndex6 = indices6.getFirst();
	    val lastIndex6 = indices6.getLast();

	    for (var i: Long = firstIndex6; i <= lastIndex6; i++) {
	        testOperatorThis(i);
	    }
	    val list2 = list.clone() as List[T];
	    for (var i: Long = firstIndex6; i <= lastIndex6; i++) {
	        testOperatorThisEqual(i, list2(lastIndex6 - (i - firstIndex6)));
//	        Console.OUT.println("### list:  " + list);
//	        Console.OUT.println("### list2: " + list2);
	    }
	    } catch (e: Exception) {
	        messages.add("EXCEPTION: " + e.typeName());
            }

	    testContains(v1);
	    testContains(v2);
	    testContains(v3);
	    testContainsAll(c1);
	    testIsEmpty();
	    testSize();
	    
	    messages.add("AFTER: List " + list.toString());
            messages.add("END: " + className + " scenario List");
        } catch (e: Exception) {
	    messages.add("ABORT: " + className + " scenario List " + e.typeName());
        }
    }


    public def scenarioSmall(v1: T, v2: T, v3: T) {T <: x10.lang.Comparable[T]} {
        messages.add("START: " + className + " scenarioSmall List");
	try {
            messages.add("BEFORE: List " + list.toString());

	    testTypeName();
	    testToString();
	    testHashCode();
	    testEquals(list);
	    testEquals(this);
	    testGetFirst();
	    testGetLast();
	    testIndices();

	    testIterator();
	    val testit1 = new TestListIterator(messages, list.iterator());
	    testit1.scenario(v1, v2);
/*
	    testIteratorFrom(middleIndex);
	    val testit2 = new TestListIterator(messages, list.iteratorFrom(15));
	    testit2.scenario(v1, v2);
*/

	    testLastIndexOf(v2);
	    testLastIndexOf(v3);
//	    testLastIndexOf(middleIndex, v2);
//	    testLastIndexOf(middleIndex, v3);

	    testRemoveAt(1);
	    testRemoveFirst();
	    testRemoveLast();
	    testReverse();
	    testSort();
	    testSort((x: T, y: T) => 0N);

/*
	    testSubList(0, 1);
*/
	    // should insert test for subList (RangedListView) here

	    testAdd(v1);

	    val c1 = new ArrayList[T]();
	    c1.add(v2);
	    c1.add(v1);
	    c1.add(v2);
	    c1.add(v1);

	    testAddAll(c1);

	    testAddAllWhere(c1, (T) => true);
	    testAddAllWhere(c1, (T) => false);

	    testClear();
	    testClone();

	    testRemove(v1);
	    testRemoveAll(c1);
	    testRemoveAllWhere((T) => false);
	    testRetainAll(c1);

	    testContains(v1);
	    testContains(v2);
	    testContains(v3);
	    testContainsAll(c1);
	    testIsEmpty();
	    testSize();
	    
	    messages.add("AFTER: List " + list.toString());
            messages.add("END: " + className + " scenarioSmall List");
        } catch (e: Exception) {
	    messages.add("ABORT: " + className + " scenarioSmall List " + e.typeName());
        }
    }

    public static def main(args:Rail[String]) {

        val msg = new ArrayList[String]();

/*
	val c = new Chunk[Long](10..20);
	val test = new TestList(msg, c);

	for (i in 10..20) {
	    c(i) = i * i;
        }

	test.scenario(123, 225, -1);

	test.printAllMessages();


	val result = test.validate("testlist.txt");
	Console.OUT.println("### VALIDATION RESULT: " + result + " ###");

*/


/*
	val c = new Chunk[Long](10..50);
	val test = new TestList(msg, c);

	for (i in 10..50) {
	    c(i) = i * i;
        }

	val c2 = c.clone();
	val c3 = c.cloneRange(20..30);

	for (i in 10..50) {
	    c2(i) = c2(i) + 1;
        }

	for (i in 20..30) {
	    c3(i) = c3(i) - 1;
        }

	Console.OUT.println("ORIGINAL [" + c.hashCode() + "] :"+ c);
	Console.OUT.println("CLONE [" + c2.hashCode() + "] :"+ c2);
	Console.OUT.println("CLONERANGE [" + c3.hashCode() + "] :"+ c3);
*/


	val c = new ArrayList[Long]();
	val test = new TestList(msg, c);

	for (i in 0..50) {
	    c.add(i * i);
        }

	val c1 = new RangedListView(c, 10..40);
	val c2 = c1.clone();
	val c3 = c1.cloneRange(20..30);

	for (i in 10..40) {
	    Console.OUT.println("c1 " + i);
	    c1(i) = c1(i) + 1;
        }

	for (i in 10..40) {
	    Console.OUT.println("c2 " + i);
	    c2(i) = c2(i) + 2;
        }

	for (i in 20..30) {
	    Console.OUT.println("c3 " + i);
	    c3(i) = c3(i) - 1;
        }

	Console.OUT.println("ARRAYLIST " + c.typeName() + " [" + c.hashCode() + "] :" + c.toString() + "");
	Console.OUT.println("ORIGINAL " + c1.typeName() + " [" + c1.hashCode() + "] :" + c1.toString() + "");
	Console.OUT.println("CLONE " + c2.typeName() + " [" + c2.hashCode() + "] :" + c2.toString() + "");
	Console.OUT.println("CLONERANGE " + c3.typeName() + " [" + c3.hashCode() + "] :" + c3.toString() + "");

    }
}
