#!/usr/bin/env groovy

import groovy.json.JsonSlurper;
import org.h2.tools.Server;
import java.sql.*;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

FLD_SEP = "\t";
WEB_PORT = "9090";
erro = System.err;
//this.binding.variables.each { key, value -> erro.println "${key} = ${value}" }

class JsonToTags{
    static List<String> toTags(String jsonText, boolean sortKeys = false ){
        def root = new JsonSlurper().parseText(jsonText);
        List<String> outList = [];
        walk(root, [], outList, sortKeys)
        return outList;
    }

    private static void walk(def node, List<String> path, List<String> outList, boolean sortKeys){
        if( node instanceof Map){
            def entries = sortKeys ? node.entrySet().sort {it.key.toString()} : node.entrySet();
            entries.each { entry ->
                walk(entry.value, path + entry.key.toString(), outList, sortKeys)
            }
            return;
        }
        if( node instanceof List){
            node.each { item -> walk(item, path, outList, sortKeys) }
            return;
        }
        String value = (node == null) ? "null" : node.toString();
        outList << (path + value).join(':');
    }

    /**

    */
    static Map<String, List<Integer>> indexFile( String filePath, boolean sortKeys = false) {
        Map<String,List<Integer>> index = [:];
        new File(filePath).eachLine
        { line, lineNumber ->
            line = line.trim()
            if( !line ) return;         // skip blank lines
            int docId = lineNumber - 1; // eachLine is 1-based
            toTags(line, sortKeys).each { tag ->
                index.computeIfAbsent(tag, { [] }) << docId;
            }
        }
        return index;
    }
}

class TagDb {

    private static final String URL = "jdbc:h2:mem:pfxdb;DB_CLOSE_DELAY=-1"
    private static final String CREATESQL = '''
    create table if not exists p(pfx text, offs int); CREATE INDEX idx_pfx ON p (pfx);'''

    private static final String INSERTSQL = "insert into p(pfx, offs) values(?, ?)";
    private Connection _conn;
    private PreparedStatement _pstm;
    private Server _websvr;

    public TagDb() {
        _conn = createDb();
        _pstm = _conn.prepareStatement(INSERTSQL);
    }

    public void startWebConsole(String port){
        _websvr = Server.createWebServer("-webPort", port, "-webAllowOthers");
        _websvr.start();/*
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("\nShutdown signal received. Stopping H2 Web Server...");
            if (_websvr != null && _websvr.isRunning(false)) {
                _websvr.stop();
                System.err.println("H2 Web Server stopped successfully.");
            }
        })); */
        System.err.println("H2 Web Console started at: " + _websvr.getURL() + "\nUse ${URL} to log in. Type CTRL+C to exit.");
    }

    public void insert(String tag, def data) {
        if(data instanceof List){
            data.each
            {i ->
                _pstm.setString(1, tag);
                _pstm.setInt(2, i);
                _pstm.executeUpdate();
            }
        }
    }

    public void close(){
        System.err.println("Closing db...");
        _pstm.close(); _conn.close();
        if (_websvr != null && _websvr.isRunning(false)) {
            _websvr.stop();
            System.err.println("H2 Web Server stopped successfully.");
        }
    }

    static Connection createDb() {

        Connection cnn = DriverManager.getConnection(URL, "sa", "")
        // Use the connection to initialize the schema
        cnn.withCloseable { c ->
            Statement stmt = c.createStatement()
            stmt.execute(CREATESQL)

            stmt.close()
            return null // Prevents implicit return of Statement/Boolean to withCloseable
        }

        // Re-open or return a fresh connection since withCloseable closes the connection
        // NOTE: In-memory DB stays alive due to DB_CLOSE_DELAY=-1
        return DriverManager.getConnection(URL, "sa", "")
    }
}

/**
 * Builds a fast secondary index map tracking line numbers to byte position offsets.
 */
static List<Long> buildLineOffsetIndex(MappedByteBuffer buffer)
{
    List<Long> index = []

    // Duplicate buffer to prevent altering the primary mapping cursor position
    MappedByteBuffer scanBuffer = (MappedByteBuffer) buffer.duplicate()
    scanBuffer.position(0)

    // The very first line always starts at byte index 0
    index.add(0L)

    while (scanBuffer.hasRemaining()) {
        long currentPos = scanBuffer.position()
        byte currentByte = scanBuffer.get()

        // When a newline character is matched, the *next* byte is a line boundary
        if (currentByte == (byte) '\n' && scanBuffer.hasRemaining()) {
            index.add((long) scanBuffer.position())
        }
    }
    return index
}

/**
 * Reads a single complete line from a target byte position offset.
 */
static String readLineFromOffset(MappedByteBuffer buffer, long offset)
{
    // Duplicate buffer so multiple threads can read distinct lines concurrently
    MappedByteBuffer readBuffer = (MappedByteBuffer) buffer.duplicate()
    readBuffer.position((int) offset)

    ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream()
    boolean foundNewline = false

    while (readBuffer.hasRemaining() && !foundNewline) {
        byte currentByte = readBuffer.get()
        if (currentByte == (byte) '\n') {
            foundNewline = true
        } else {
            lineBuffer.write(currentByte)
        }
    }

    String result = lineBuffer.toString(StandardCharsets.UTF_8.name())
    return result.endsWith('\r') ? result.substring(0, result.length() - 1) : result
}


void process()
{
    if( !(this.binding.variables['named_args'] && this.binding.variables['named_args'].size() > 0))
    {
        demo(); return;
    }

    boolean printData = named_args['print'] && named_args['print'] == "true"
    def fileName = named_args['file'];
    def index = JsonToTags.indexFile(fileName)
    def db = new TagDb();
    int cnt = 1;

    RandomAccessFile raf = new RandomAccessFile(new File(fileName), "r")
    FileChannel channel = raf.getChannel()
    MappedByteBuffer globalMemMap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size())

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("\nShutdown signal received. Stopping ...");
            db.close();
            erro.println("Closing memory file")
            channel.close();
            raf.close();
        }));
    try{
        index.sort().each { tag, ids ->
            db.insert(tag, ids);
            if( printData ){print(tag + FLD_SEP); println(ids)}
            if( cnt % 100 == 0 ){ erro.print( "\rtag count: " + cnt ); }
            cnt++
        }
        erro.print( "\rtag count: " + cnt-1  + "\n");

        long startTime = System.nanoTime()
        List<Long> offsetIndex = buildLineOffsetIndex(globalMemMap)
        long duration = System.nanoTime() - startTime
/*
        erro.println "--- Secondary Index Map Layout ---"
        offsetIndex.eachWithIndex { byteOffset, lineNum ->
            erro.println "Line #${lineNum} starts at byte position: ${byteOffset}"
        }
        erro.println ""
        */
        db.startWebConsole(WEB_PORT);

        // 4. Instant point-lookups (O(1) complexity lookup)
        erro.println "--- Running O(1) Instant Line Lookups ---"

        // Fetch line 2 instantly
        int targetLineA = 2
        long targetOffsetA = offsetIndex[targetLineA]
        erro.println "Fetching line ${targetLineA} from offset ${targetOffsetA}..."
        erro.println "Result: \"${readLineFromOffset(globalMemMap, targetOffsetA)}\"\n"

        // Fetch line 4 instantly
        int targetLineB = 4
        long targetOffsetB = offsetIndex[targetLineB]
        erro.println "Fetching line ${targetLineB} from offset ${targetOffsetB}..."
        erro.println "Result: \"${readLineFromOffset(globalMemMap, targetOffsetB)}\"\n"

        while(true){
            try{Thread.sleep(Long.MAX_VALUE);}
            catch(Exception e){erro.println("Exiting server thread"); break}
        }
    } finally {
        erro.println("FINALLY BLOCK CALLED")
    }

}

process()


void demo()
{
    // demo
    def sampleFile = File.createTempFile("docs", ".jsonl")
    sampleFile.text = '''\
    {"a":1, "b":{"c":2}, "d":[3,4,{"e":"zz"}]}
    {"a":1, "x":99, "g": null}
    {"b":{"c":2}, "x":99}
    ''';
    def index = JsonToTags.indexFile(sampleFile.absolutePath)
    println( "Tag index ${sampleFile.absolutePath}:")
    index.sort().each { tag, ids ->
        println( " ${tag.padRight(12)} -> docIds: ${ids}")
    }
    sampleFile.delete()
}

/*
jq -c '.[] ' ~/test-data/chainlist.json > ~/ephemeral/test.json
CLASSPATH=$CLASSPATH:~/progs/h2/bin/*  gi.sh  ~/work/groovy/jsonToTags.groovy file=/home/oo/ephemeral/test.json > ~/ephemeral/dump.txt
CLASSPATH="/home/oo/progs/h2/bin/*" ~/progs/groovy-5.0.6/bin/groovy  ~/work/groovy/jsonToTags.groovy file=/home/oo/ephemeral/test.json
 gi.sh  ~/work/groovy/jsonToTags.groovy file=/home/oo/ephemeral/test.json print=true > ~/ephemeral/dump.txt

create alias if not exists to_string for 'com.entity.tools.H2Functions.toString'
SELECT prefix, to_string(offsets) FROM PFX ;
*/

/*

SELECT p1.pfx, p2.pfx, p2.offs
FROM p p1, p p2 use index (idx_pfx)
 where p1.pfx like 'parent:type:L%'
and  p2.pfx like 'name:A%'
and p1.offs = p2.offs

*/