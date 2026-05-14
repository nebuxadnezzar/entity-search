#!/usr/bin/env groovy

import groovy.json.JsonSlurper;
import org.h2.tools.Server;
import java.sql.*;

FLD_SEP = "\t";
WEB_PORT = "9090";
erro = System.err;
this.binding.variables.each { key, value -> erro.println "${key} = ${value}" }

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
    private static final String CREATESQL = "create table if not exists pfx(id integer primary key auto_increment, prefix text, offsets other);"
    private static final String INSERTSQL = "insert into pfx(prefix, offsets) values(?, ?)";
    private Connection _conn;
    private PreparedStatement _pstm;
    private Server _websvr;

    public TagDb() {
        _conn = createDb();
        _pstm = _conn.prepareStatement(INSERTSQL);
    }

    public void startWebConsole(String port){
        _websvr = Server.createWebServer("-webPort", port, "-webAllowOthers");
        _websvr.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("\nShutdown signal received. Stopping H2 Web Server...");
            if (_websvr != null && _websvr.isRunning(false)) {
                _websvr.stop();
                System.err.println("H2 Web Server stopped successfully.");
            }
        }));
        System.err.println("H2 Web Console started at: " + _websvr.getURL() + "\nUse ${URL} to log in. Type CTRL+C to exit.");
    }

    public boolean insert(String tag, def data) {
        _pstm.setString(1, tag);
        _pstm.setObject(2, data);
        return _pstm.executeUpdate();
    }

    public void close(){_pstm.close();}

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

if( this.binding.variables['named_args'] && this.binding.variables['named_args'].size() > 0)
{
    def index = JsonToTags.indexFile(named_args['file'])
    def db = new TagDb();
    int cnt = 1;
    index.sort().each { tag, ids ->
        db.insert(tag, ids);
        print(tag + FLD_SEP); println(ids)
        erro.print( "\rdocument count: " + cnt++ );
    }
    erro.print( "\rdocument count: " + cnt-1  + "\n");
    db.startWebConsole(WEB_PORT);
    erro.println("Closing db");
    db.close();
} else
{
    demo()
}

/*
jq -c '.[] ' ~/test-data/chainlist.json > ~/ephemeral/test.json
CLASSPATH=$CLASSPATH:~/progs/h2/bin/*  gi.sh  ~/work/groovy/jsonToTags.groovy file=/home/oo/ephemeral/test.json > ~/ephemeral/dump.txt

create alias if not exists to_string for 'com.entity.tools.H2Functions.toString'
SELECT prefix, to_string(offsets) FROM PFX ;
*/

