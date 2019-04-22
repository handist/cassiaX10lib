package samples.rangedList;
import x10.util.*;
import cassia.util.*;

public class TestRangedListView[T] extends TestAny {

    private val view: RangedListView[T];

    public def this(messages: ArrayList[String], view: RangedListView[T]) {
        super(messages, view);
	this.view = view;
    }

// class RangedListView[T]

    public def testToChunk(newRange: LongRange) {
        messages.add("CALL: " + className + "::toChunk " + "(" + newRange + ")");
	try {
            messages.add("BEFORE: RangedListView " + view.toString());
	    val rc = view.toChunk(newRange);
	    messages.add("AFTER: RangedListView " + view.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testLast() {
        messages.add("CALL: " + className + "::last " + "()");
	try {
            messages.add("BEFORE: RangedListView " + view.toString());
	    val rc = view.last();
	    messages.add("AFTER: RangedListView " + view.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testEach(op: (T)=>void) {
        messages.add("CALL: " + className + "::each " + "(" + op + ")");
	try {
            messages.add("BEFORE: RangedListView " + view.toString());
	    view.each(op);
	    messages.add("AFTER: RangedListView " + view.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testEach(range:LongRange, op: (T)=>void) {
        messages.add("CALL: " + className + "::each " + "(" + range + ", " + op + ")");
	try {
            messages.add("BEFORE: RangedListView " + view.toString());
	    view.each(range, op);
	    messages.add("AFTER: RangedListView " + view.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testEach[U](op: (T, Receiver[U])=>void, receiver: Receiver[U]) {
        messages.add("CALL: " + className + "::each " + "(" + op + ", " + receiver + ")");
	try {
            messages.add("BEFORE: RangedListView " + view.toString());
	    view.each(op, receiver);
	    messages.add("AFTER: RangedListView " + view.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testEach[U](range:LongRange, op: (T, Receiver[U])=>void, receiver: Receiver[U]) {
        messages.add("CALL: " + className + "::each " + "(" + range + ", " + op + ", " + receiver + ")");
	try {
            messages.add("BEFORE: RangedListView " + view.toString());
	    view.each(op, receiver);
	    messages.add("AFTER: RangedListView " + view.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

// Scenario

    public def scenario(v1: T, v2: T, v3: T, range1: LongRange, range2: LongRange, op1: (T)=>T, op2: (T)=>T) {T <: x10.lang.Comparable[T]} {
        messages.add("START: " + className + " scenario RangedListView");
	try {
            messages.add("BEFORE: RangedListView " + view.toString());

	    testToChunk(range1);

	    val view2 = view.clone();
	    val view3 = view.cloneRange(range1);
	    val view4 = view3.cloneRange(range2);

	    messages.add("CLONE: " + view2);
	    messages.add("CLONERANGE(" + range1 + "): " + view3);
	    messages.add("CLONERANGE(" + range2 + ") OF ABOVE: " + view4);
	    messages.add("DO OPERATIONS");	    

	    val it2 = view2.iterator();
	    val it3 = view3.iterator();
	    val it4 = view4.iterator();

	    while (it2.hasNext()) {
	        val idx = it2.nextIndex();
		view2(idx) = op1(view2(idx));
	    }

	    while (it3.hasNext()) {
	        val idx = it3.nextIndex();
		view3(idx) = op2(view3(idx));
	    }

	    while (it4.hasNext()) {
	        val idx = it4.nextIndex();
		view4(idx) = op1(view4(idx));
	    }

	    messages.add("ORIGINAL: " + view);
	    messages.add("CLONE OP1: " + view2);
	    messages.add("CLONERANGE(" + range1 + ") OP2: " + view3);
	    messages.add("CLONERANGE(" + range2 + ") OF ABOVE OP1: " + view4);

	    testLast();
	    testEach((v: T) => { messages.add("EACH: " + v); });

	    val receiver = new SimpleReceiver[T]();
	    testEach((v: T, r: Receiver[T]) => {
                val idx = view.indexOf(v);
                if (idx % 2 == 1) {
		    messages.add("EACH: " + idx + ", " + v + " RECEIVE");
                    r.receive(v);
                } else {
		    messages.add("EACH: " + idx + ", " + v + " SKIP");
                }
            }, receiver);
	    messages.add("RECEIVER: " + receiver);
	    testEach((v: T, r: Receiver[T]) => {
                val idx = view.indexOf(v);
                if (idx % 2 == 0) {
		    messages.add("EACH: " + idx + ", " + v + " RECEIVE");
                    r.receive(v);
                } else {
		    messages.add("EACH: " + idx + ", " + v + " SKIP");
                }
            }, receiver);
	    messages.add("RECEIVER: " + receiver);

	    messages.add("AFTER: RangedListView " + view.toString());
            messages.add("END: " + className + " scenario RangedListView");
        } catch (e: Exception) {
	    messages.add("ABORT: " + className + " scenario RangedListView " + e.typeName());
        }
    }

// 

    public static def main(args: Rail[String]) {

        val command = "TestRangedListView";
        if (args.size != 3) {
	    Console.OUT.println("Usage: ");
	    Console.OUT.println("    "  + command + " trace [filename1] [filename2]");
	    Console.OUT.println("        Write test log for the next validation.");
	    Console.OUT.println("    "  + command + " validate [filename1] [filename2]");
	    Console.OUT.println("        Check equality of test log with the previous trace.");
	} else if (args(0).equals("trace")) {
	    run_trace(args(1), args(2));
	} else if (args(0).equals("validate")) {
	    run_validate(args(1), args(2));
	}

    }


    public static def run_trace(filename1: String, filename2: String) {

        val msg1 = new ArrayList[String]();
        val msg2 = new ArrayList[String]();

	val c1 = new Chunk[Long](10..100);
	for (i in 10..100) {
	    c1(i) = i * i;
        }

	val c2 = new ArrayList[Long]();
	for (i in 0..100) {
	    c2.add(i * i);
	}

	val r1 = new RangedListView(c1, 20..80);
	val r2 = new RangedListView(c2, 20..80);
	val test1 = new TestRangedListView(msg1, r1);
	val test2 = new TestRangedListView(msg2, r2);

	test1.scenario(123, 225, -1, 30..70, 50..60, (v: Long)=>{ return v + 1; }, (v: Long)=>{ return v - 1; });
	test2.scenario(123, 225, -1, 30..70, 50..60, (v: Long)=>{ return v + 1; }, (v: Long)=>{ return v - 1; });
	test1.printAllMessages(filename1);
	test2.printAllMessages(filename1);
//	val result1 = test1.validate(filename1);
//	val result2 = test2.validate(filename1);
//	Console.OUT.println("### VALIDATION RESULT: RangedListView to Chunk[Long] is " + result1 + " ###");
//	Console.OUT.println("### VALIDATION RESULT: RangedListView to ArrayList[Long] is " + result2 + " ###");

	val msgList1 = new ArrayList[String]();
	val msgList2 = new ArrayList[String]();
	val testList1 = new TestList(msgList1, r1);
	val testList2 = new TestList(msgList2, r2);

	testList1.scenario(123, 225, -1);
	testList2.scenario(123, 225, -1);
	testList1.printAllMessages(filename2);
	testList2.printAllMessages(filename2);
//	val resultList1 = testList1.validate(filename2);
//	val resultList2 = testList2.validate(filename2);
//	Console.OUT.println("### VALIDATION RESULT: List interface of RangedListView to Chunk[Long] is " + resultList1 + " ###");
//	Console.OUT.println("### VALIDATION RESULT: List interface of RangedListView to ArrayList[Long] is " + resultList2 + " ###");

    }


    public static def run_validate(filename1: String, filename2: String) {

        val msg1 = new ArrayList[String]();
        val msg2 = new ArrayList[String]();

	val c1 = new Chunk[Long](10..100);
	for (i in 10..100) {
	    c1(i) = i * i;
        }

	val c2 = new ArrayList[Long]();
	for (i in 0..100) {
	    c2.add(i * i);
	}

	val r1 = new RangedListView(c1, 20..80);
	val r2 = new RangedListView(c2, 20..80);
	val test1 = new TestRangedListView(msg1, r1);
	val test2 = new TestRangedListView(msg2, r2);

	test1.scenario(123, 225, -1, 30..70, 50..60, (v: Long)=>{ return v + 1; }, (v: Long)=>{ return v - 1; });
	test2.scenario(123, 225, -1, 30..70, 50..60, (v: Long)=>{ return v + 1; }, (v: Long)=>{ return v - 1; });
//	test1.printAllMessages(filename1);
//	test2.printAllMessages(filename1);
	val result1 = test1.validate(filename1);
	val result2 = test2.validate(filename1);
	Console.OUT.println("### VALIDATION RESULT: RangedListView to Chunk[Long] is " + result1 + " ###");
	Console.OUT.println("### VALIDATION RESULT: RangedListView to ArrayList[Long] is " + result2 + " ###");

	val msgList1 = new ArrayList[String]();
	val msgList2 = new ArrayList[String]();
	val testList1 = new TestList(msgList1, r1);
	val testList2 = new TestList(msgList2, r2);

	testList1.scenario(123, 225, -1);
	testList2.scenario(123, 225, -1);
//	testList1.printAllMessages(filename2);
//	testList2.printAllMessages(filename2);
	val resultList1 = testList1.validate(filename2);
	val resultList2 = testList2.validate(filename2);
	Console.OUT.println("### VALIDATION RESULT: List interface of RangedListView to Chunk[Long] is " + resultList1 + " ###");
	Console.OUT.println("### VALIDATION RESULT: List interface of RangedListView to ArrayList[Long] is " + resultList2 + " ###");

    }

}
