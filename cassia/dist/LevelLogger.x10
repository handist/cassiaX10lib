package cassia.dist;

import x10.util.StringBuilder;

import x10.compiler.Inline;
import x10.compiler.NativeRep;
import x10.compiler.Native;

class LevelLogger {

    static val DEFAULT_LEVEL:Int = getDefaultLevel();
    static val DEFAULT_LEVEL_ENV:String = "X10_LOG_LEVEL";

    static val LEVEL_ALL:Int = 7n;
    static val LEVEL_TRACE:Int = 6n;
    static val LEVEL_DEBUG:Int = 5n;
    static val LEVEL_INFO:Int = 4n;
    static val LEVEL_WARN:Int = 3n;
    static val LEVEL_ERROR:Int = 2n;
    static val LEVEL_FATAL:Int = 1n;
    static val LEVEL_NONE:Int = 0n;

    private static val LEVEL_STRINGS:Rail[String] = ["NONE", "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE","ALL"];

    private static def getDefaultLevel():Int {
        var v:String = System.getenv(DEFAULT_LEVEL_ENV);
        if (v == null) {
            return LEVEL_INFO;
        }

        val prefix = "LEVEL_";
        val prefixLength = prefix.length();
        if (v.length() > prefixLength && prefix.equalsIgnoreCase(v.substring(0n,prefixLength))) {
            v = v.substring(prefixLength);
        }
        //Console.ERR.println(v);
        for (i in LEVEL_STRINGS.range()) {
            if(LEVEL_STRINGS(i).equalsIgnoreCase(v)) {
                return i as Int;
            }
        }
        return LEVEL_INFO;
    }

    @Inline
    private static def logLevelString(logLevel:Int):String = LEVEL_STRINGS(logLevel);

    private static def getLastPartOfDotName(name:String):String {
        val index = name.lastIndexOf(".");
        if (index == -1n) {
            return name;
        } else if (index >= name.length() - 1n) {
            return name;
        } else {
            return name.substring(index + 1n);
        }
    }


    public def this(name:String) {
        this(name, DEFAULT_LEVEL);
    }


    public def this(name:String, logLevel:Int) {
        this(name, logLevel, getLastPartOfDotName(name));
    }


    public def this(name:String, logLevel:Int, outputName:String) {
        this.name = name;
        this.logLevel = logLevel;
        this.outputName = outputName;
    }

    public def isTraceEnabled():Boolean = LEVEL_TRACE <= logLevel;
    public def isDebugEnabled():Boolean = LEVEL_DEBUG <= logLevel;
    public def isInfoEnabled() :Boolean = LEVEL_INFO <= logLevel;
    public def isWarnEnabled() :Boolean = LEVEL_WARN <= logLevel;
    public def isErrorEnabled():Boolean = LEVEL_ERROR <= logLevel;
    public def isFatalEnabled():Boolean = LEVEL_FATAL <= logLevel;

    public def trace(message:Any):void {
        log(LEVEL_TRACE, message);
    }

    public def trace(message:Any, t:CheckedThrowable):void {
        log(LEVEL_TRACE, message, t);
    }

    public def debug(message:Any):void {
        log(LEVEL_DEBUG, message);
    }

    public def debug(message:Any, t:CheckedThrowable):void {
        log(LEVEL_DEBUG, message, t);
    }

    public def info(message:Any):void {
        log(LEVEL_INFO, message);
    }

    public def info(message:Any, t:CheckedThrowable):void {
        log(LEVEL_INFO, message, t);
    }

    public def warn(message:Any):void {
        log(LEVEL_WARN, message);
    }

    public def warn(message:Any, t:CheckedThrowable):void {
        log(LEVEL_WARN, message, t);
    }


    public def error(message:Any):void {
        log(LEVEL_ERROR, message);
    }


    public def error(message:Any, t:CheckedThrowable):void {
        log(LEVEL_ERROR, message, t);
    }


    public def fatal(message:Any):void {
        log(LEVEL_FATAL, message);
    }


    public def fatal(message:Any, t:CheckedThrowable):void {
        log(LEVEL_FATAL, message, t);
    }


    public def toString():String = "ConsoleLog<" + name + "@" + logLevelString(logLevel) + ">";


    private val name:String;

    private val outputName:String;

    private var logLevel:Int;


    private def setLogLevel(newLevel:Int):void {
        if (newLevel < LEVEL_NONE) {
            logLevel = LEVEL_NONE;
        } else if (newLevel > LEVEL_ALL) {
            logLevel = LEVEL_ALL;
        } else {
            logLevel = newLevel;
        }
    }


    private def isLogEnabled(level:Int):Boolean = level <= logLevel;

    @Inline
    private def log(level:Int, message:Any):void {
        if (!isLogEnabled(level)) {
            return;
        }
        val sb = new StringBuilder();
        sb.add(logLevelString(level));
        sb.add(" ");
        sb.add(outputName);
        sb.add(": ");
        sb.add(message);

        Console.OUT.println(sb.result());
    }

    @Inline
    private def log(level:Int, message:Any, t:CheckedThrowable):void {
        if (!isLogEnabled(level)) {
            return;
        }
        if (t == null) {
            log(level, message);
            return;
        }
        val sb = new StringBuilder();
        sb.add(logLevelString(level));
        sb.add(" ");
        sb.add(outputName);
        sb.add(": ");
        sb.add(message);
        sb.add(", cause: ");
        sb.add(t.getCause());
        Console.OUT.println(sb.result());
        t.printStackTrace();
    }

}
