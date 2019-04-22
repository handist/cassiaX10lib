package samples.rangedList;
import x10.util.*;

public class TestListIterator[T] extends TestAny {

    private val iterator: ListIterator[T];

    public def this(messages: ArrayList[String], iterator: ListIterator[T]) {
    	super(messages, iterator);
        this.iterator = iterator;
    }

// Interface ListIterator

    public def testAdd(v: T) {
        messages.add("CALL: " + className + "::add " + "(" + v + ")");
	try {
            messages.add("BEFORE: ListIterator " + iterator.toString());
	    iterator.add(v);
	    messages.add("AFTER: ListIterator " + iterator.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testHasNext() {
        messages.add("CALL: " + className + "::hasNext " + "()");
	try {
            messages.add("BEFORE: ListIterator " + iterator.toString());
	    val rc = iterator.hasNext();
	    messages.add("AFTER: ListIterator " + iterator.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testHasPrevious() {
        messages.add("CALL: " + className + "::hasPrevious " + "()");
	try {
            messages.add("BEFORE: ListIterator " + iterator.toString());
	    val rc = iterator.hasPrevious();
	    messages.add("AFTER: ListIterator " + iterator.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testNext() {
        messages.add("CALL: " + className + "::next " + "()");
	try {
            messages.add("BEFORE: ListIterator " + iterator.toString());
	    val rc = iterator.next();
	    messages.add("AFTER: ListIterator " + iterator.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testNextIndex() {
        messages.add("CALL: " + className + "::nextIndex " + "()");
	try {
            messages.add("BEFORE: ListIterator " + iterator.toString());
	    val rc = iterator.nextIndex();
	    messages.add("AFTER: ListIterator " + iterator.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testPrevious() {
        messages.add("CALL: " + className + "::previous " + "()");
	try {
            messages.add("BEFORE: ListIterator " + iterator.toString());
	    val rc = iterator.previous();
	    messages.add("AFTER: ListIterator " + iterator.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testPreviousIndex() {
        messages.add("CALL: " + className + "::previousIndex " + "()");
	try {
            messages.add("BEFORE: ListIterator " + iterator.toString());
	    val rc = iterator.previousIndex();
	    messages.add("AFTER: ListIterator " + iterator.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testSet(v: T) {
        messages.add("CALL: " + className + "::set " + "(" + v + ")");
	try {
            messages.add("BEFORE: ListIterator " + iterator.toString());
	    iterator.set(v);
	    messages.add("AFTER: ListIterator " + iterator.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

// Interface CollectionIterator

    public def testRemove() {
        messages.add("CALL: " + className + "::remove " + "()");
	try {
            messages.add("BEFORE: ListIterator " + iterator.toString());
	    iterator.remove();
	    messages.add("AFTER: ListIterator " + iterator.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

// Interface Iterator

// Interface Any


// Scenario 

    public def scenario(v1: T, v2: T) {
        messages.add("START: " + className + " scenario ListIterator");
        try {
            messages.add("BEFORE: ListIterator " + iterator.toString());
	    testTypeName();
	    testToString();
	    testHashCode();
	    testEquals(iterator);
	    testEquals(this);
	    testNext();
	    testNext();
	    testNext();
	    testNext();
	    testNext();
	    testPrevious();
	    testPrevious();
	    testPrevious();
	    testPreviousIndex();
	    testPreviousIndex();
	    testPreviousIndex();
	    testNextIndex();
	    testNextIndex();
	    testNextIndex();
	    testNext();
	    testNext();
	    testNext();
	    testNext();
	    testNext();
	    testNext();
	    testNext();
	    testNext();
	    testNext();
	    testPreviousIndex();
	    testPreviousIndex();
	    testPreviousIndex();
	    testNextIndex();
	    testNextIndex();
	    testNextIndex();
	    testNextIndex();
	    testNextIndex();
	    testSet(v2);
	    testPrevious();
	    testPrevious();
	    testPrevious();
	    testSet(v1);
	    testHasNext();
	    testHasPrevious();
	    testPrevious();
	    testHasPrevious();
	    testPrevious();
	    testHasPrevious();
	    testPrevious();
	    testHasPrevious();
	    testPrevious();
	    testHasPrevious();
	    testPrevious();
	    testHasPrevious();
	    testPrevious();
	    testHasPrevious();
	    testPrevious();
	    testHasPrevious();
	    testPrevious();
	    testHasPrevious();
	    testPrevious();
	    testHasPrevious();
	    testPrevious();
	    testSet(v2);
	    testHasPrevious();
	    testPrevious();
	    testHasNext();
	    testNext();
	    testHasNext();
	    testNext();
	    testHasNext();
	    testNext();
	    testHasNext();
	    testNext();
	    testHasNext();
	    testNext();
	    testHasNext();
	    testNext();
	    testHasNext();
	    testNext();
	    testHasNext();
	    testNext();
	    testHasNext();
	    testNext();
	    testHasNext();
	    testNext();
	    testHasNext();
	    testNext();
	    testHasNext();
	    testNext();
	    testHasNext();
	    testNext();
	    messages.add("AFTER: ListIterator " + iterator.toString());
            messages.add("END: " + className + " scenario ListIterator");
        } catch (e: Exception) {
            messages.add("ABORT: " + className + " scenario ListIterator " + e.typeName());
        }
    }

}
