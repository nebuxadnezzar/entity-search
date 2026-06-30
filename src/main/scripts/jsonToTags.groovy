#!/usr/bin/env groovy

import groovy.json.JsonSlurper;
import java.sql.*;
import java.util.regex.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

FLD_SEP = "\t";
WEB_PORT = "9090";
erro = System.err;
//this.binding.variables.each { key, value -> erro.println "${key} = ${value}" }

class JsonToTags{

    static List<String> tokenize(String s){
        return !s ? [] :
        s.replaceAll("[^\\w\\s]+", ' ').toLowerCase()
         .replaceAll('\s+(and|y|the|a|d|s|ll|re|ve|your|yours)\s+', ' ')
         .replaceAll('\s+', ' ').trim().split(' ')
    }
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
        tokenize(value).each{ v -> outList << (path + v).join(':')}
        //outList << (path + value).join(':');
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
                //System.err.println("TAG: ${tag}")
                index.computeIfAbsent(tag.toLowerCase(), { [] }) << docId;
            }
        }
        return index;
    }
}

class TagDb {

    private final String [] keys;
    private final Map<String,intArray> index = [:];

    public TagDb(Map<String, List<Integer>> indexMap){
        this.keys = (indexMap.keySet() as String []).sort();
        indexMap.each { k, v ->
            index[k] = new intArray(v);
        }
        System.err.println("KEYS: ${this.index.size()}")
    }

    private class intArray{
        private final int [] offs;
        private intArray (List<Integer> lst) { offs = lst as int[]; }
    }

    public int [] search(Map query){

        //def lst = fuzzySearch(keys, "chain:AC?");
        //System.err.println("FOUND: ${lst}")
        return searchHelper(query, keys, index);
    }

    private static int [] searchHelper(Map query, String [] keys, Map<String, intArray> index){

        int [] result;
        query.each {k, v ->
            System.err.println("${k} -> ${v}")
            v.each { item ->
                if(item instanceof Map){
                    System.err.println("ITEM: ${item}")
                    result = setop(k, result, searchHelper(item, keys, index))
                } else if(item instanceof String) {
                    System.err.println("SEARCING FOR:${item}")
                    def tmp = [];
                    fuzzySearch(keys, item.toLowerCase()).each { key ->
                        System.err.println("-> ${key}")
                        //result = setop(k, result, index[key].offs)
                        tmp.addAll(index[key].offs)
                        tmp.sort()
                        //System.err.println "\nTMP: ${tmp}"
                    }
                    result = setop(k, result, tmp as int[])
                }
            }

        }
        //System.err.println("!!! RESULT: ${result}")
        return result;
    }

    private static int[] setop(String op, int[] arr1, int[] arr2){

        switch(op){
            case "and": return findIntersection(arr1, arr2);
            case "or" : return findUnion(arr1, arr2);
            default:
                return findUnion(arr1, arr2);
        }
    }

    public static int[] findIntersection(int[] arr1, int[] arr2) {
        System.err.println("1. FIND INTERSECTION: ${arr1} ${arr2}")
        if(arr1 == null){ return arr2;}
        if(arr2 == null){ return arr1;}

        // 1. Sort both arrays to allow two-pointer traversal
        //Arrays.sort(arr1);
        //Arrays.sort(arr2);

        int i = 0, j = 0, k = 0;
        int[] temp = new int[Math.min(arr1.length, arr2.length)];

        // 2. Traverse arrays together
        while (i < arr1.length && j < arr2.length) {
            if (arr1[i] < arr2[j]) {
                i++;
            } else if (arr1[i] > arr2[j]) {
                j++;
            } else {
                // Found a match. Skip duplicates to ensure distinct results.
                if (k == 0 || temp[k - 1] != arr1[i]) {
                    temp[k++] = arr1[i];
                }
                i++;
                j++;
            }
        }
        System.err.println("2. FIND INTERSECTION: ${temp} ${k}")
        // 3. Trim the temporary array to the actual number of matched elements
        return Arrays.copyOf(temp, k);
    }

    public static int[] findUnion(int[] arr1, int[] arr2) {
        System.err.println("1. FIND UNION: ${arr1} ${arr2}")
        if(arr1 == null){ return arr2;}
        if(arr2 == null){ return arr1;}
        int i = 0, j = 0, k = 0;

        // Allocate a temporary array large enough to hold all unique elements
        int[] temp = new int[arr1.length + arr2.length];

        // Process both arrays using two pointers
        while (i < arr1.length && j < arr2.length) {
            if (arr1[i] < arr2[j]) {
                // Add from arr1 if it's smaller and not a duplicate
                if (k == 0 || temp[k - 1] != arr1[i]) {
                    temp[k++] = arr1[i];
                }
                i++;
            } else if (arr2[j] < arr1[i]) {
                // Add from arr2 if it's smaller and not a duplicate
                if (k == 0 || temp[k - 1] != arr2[j]) {
                    temp[k++] = arr2[j];
                }
                j++;
            } else {
                // Elements are equal; add once and advance both pointers
                if (k == 0 || temp[k - 1] != arr1[i]) {
                    temp[k++] = arr1[i];
                }
                i++;
                j++;
            }
        }

        // Copy remaining elements from arr1, if any
        while (i < arr1.length) {
            if (k == 0 || temp[k - 1] != arr1[i]) {
                temp[k++] = arr1[i];
            }
            i++;
        }

        // Copy remaining elements from arr2, if any
        while (j < arr2.length) {
            if (k == 0 || temp[k - 1] != arr2[j]) {
                temp[k++] = arr2[j];
            }
            j++;
        }
        System.err.println("2. FIND UNION: ${temp} ${k}")
        // Trim the temporary array to the exact number of added unique elements
        return Arrays.copyOf(temp, k);
    }

    public static List<String> fuzzySearch(String[] sortedArray, String wildcardPattern) {
        List<String> matches = new ArrayList<>();
        if (sortedArray == null || sortedArray.length == 0 || wildcardPattern == null) {
            return matches;
        }

        // 1. Convert Wildcard pattern to standard Java Regex
        // Escape special regex chars, map '*' to '.*', and '?' to '.'
        String regex = "^" + Pattern.quote(wildcardPattern)
                                    .replace('*', "\\E.*\\Q")
                                    .replace('?', "\\E.\\Q") + '$';
        // Clean up empty \Q\E blocks caused by substitutions
        regex = regex.replace("\\Q\\E", "");
        Pattern pattern = Pattern.compile(regex);

        // 2. Extract a literal leading prefix if the pattern starts with a normal character
        String prefix = extractLiteralPrefix(wildcardPattern);

        int startIndex = 0;
        int endIndex = sortedArray.length;

        // 3. Optimization: If there is a fixed prefix, use Binary Search to find the starting bound
        if (!prefix.isEmpty()) {
            int searchIdx = Arrays.binarySearch(sortedArray, prefix);
            if (searchIdx < 0) {
                // If not found exactly, binarySearch returns (-(insertion point) - 1)
                startIndex = -searchIdx - 1;
            } else {
                startIndex = searchIdx;
            }
        }

        // 4. Scan and filter the range
        for (int i = startIndex; i < endIndex; i++) {
            String current = sortedArray[i];

            // Optimization: If we have a prefix and the array element no longer starts with it,
            // we can stop scanning immediately because the array is sorted alphabetically.
            if (!prefix.isEmpty() && !current.startsWith(prefix)) {
                break;
            }

            // Check if the current element satisfies the fuzzy regex
            if (pattern.matcher(current).matches()) {
                matches.add(current);
            }
        }

        return matches;
    }
    /**
     * Helper to grab any fixed leading characters before a wildcard appears.
     */
    private static String extractLiteralPrefix(String pattern) {
        StringBuilder sb = new StringBuilder();
        for (char c : pattern.toCharArray()) {
            if (c == '*' || c == '?') {
                break;
            }
            sb.append(c);
        }
        return sb.toString();
    }
}

class HttpSvr {

    private int port = 8080
    private Closure onPost;
    private ExecutorService pool = determineExecutor()
    private HttpServer server = HttpServer.create(new InetSocketAddress(port), 0)

    public HttpSvr(int port, Closure onPost){
        this.port = port;
        this.onPost = onPost;
        init();
    }
    // 1. Determine the best available executor service at runtime
    ExecutorService determineExecutor() {
        if (Executors.metaClass.respondsTo(Executors, "newVirtualThreadPerTaskExecutor")) {
            System.err.println "JDK 21+ detected. Using Virtual Thread pool."
            return Executors.newVirtualThreadPerTaskExecutor()
        } else {
            System.err.println "Older JDK detected. Falling back to Cached Thread pool."
            return Executors.newCachedThreadPool()
        }
    }

    private void init(){
        server.with {
            setExecutor(pool)

            createContext("/hello") { exchange ->
                byte[] response = "Hello from a safe, resilient server!".getBytes()
                exchange.responseHeaders.add("Content-Type", "text/plain")
                exchange.sendResponseHeaders(200, response.length)
                exchange.responseBody.withStream { out -> out.write(response) }
            }

            createContext("/q") { exchange ->
                if ("POST".equalsIgnoreCase(exchange.requestMethod)) {

                    // 2. Read the request body stream safely using withStream
                    def requestBody = ""
                    exchange.requestBody.withStream { stream ->
                        requestBody = stream.text // Groovy extension to easily read stream to String
                    }

                    System.err.println "Received POST Data: ${requestBody}"

                    // 3. Prepare the response
                    def responseText = "Data received successfully!"

                    def headers = exchange.responseHeaders
                    headers.set("Content-Type", "application/json; charset=UTF-8")

                    // send data in transfer chunks setting content length to 0 (chunked transfer)
                    exchange.sendResponseHeaders(200, 0)

                    // 4. Write the response body stream safely using withStream
                    exchange.responseBody.withStream { outStream ->
                        if(onPost){
                            onPost(requestBody, outStream)
                        } else {
                            outStream.write(responseText.getBytes("UTF-8"))
                        }
                    }
                } else {
                    // Method Not Allowed
                    exchange.sendResponseHeaders(405, -1)
                }
            }
        }

        // 2. Register the JVM Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread({
           shutdown();
        }))
    }

    public void shutdown(){
        System.err.println "\nShutdown signal received. Commencing graceful shutdown of HTTP Server..."

            // Stop the HTTP server immediately (0 second delay for new requests)
            server.stop(0)
            System.err.println "HTTP server stopped."

            // Disable new tasks from being submitted to the pool
            pool.shutdown()

            try {
                // Wait up to 5 seconds for existing active tasks to finish
                if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.err.println "Tasks did not finish in time. Forcing pool shutdown..."
                    pool.shutdownNow() // Cancel currently executing tasks
                }
            } catch (InterruptedException ie) {
                pool.shutdownNow()
                Thread.currentThread().interrupt()
            }

        println "Shutdown complete. Goodbye."
    }
    public void start(){server.start();}
    public void stop(){shutdown();}
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
    def tdb = new TagDb(index);
    int cnt = 0;

    RandomAccessFile raf = new RandomAccessFile(new File(fileName), "r")
    FileChannel channel = raf.getChannel()
    MappedByteBuffer globalMemMap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size())

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            erro.println("Closing memory file")
            channel.close();
            raf.close();

        }));
    try{
        index.sort().each { tag, ids ->
            if( printData ){print(tag + FLD_SEP); println(ids)}
            if( cnt % 100 == 0 ){ erro.print( "\rtag count: " + cnt ); }
            cnt++
        }
        erro.print( "\rtag count: " + cnt-1  + "\n");

        long startTime = System.nanoTime()
        List<Long> offsetIndex = buildLineOffsetIndex(globalMemMap)
        long duration = System.nanoTime() - startTime
        erro.println "Index generation complete in ${duration / 1_000_000.0} ms."
        erro.println "Total lines indexed: ${offsetIndex.size()}\n"

        // 4. Instant point-lookups (O(1) complexity lookup)
        erro.println "--- Running O(1) Instant Line Lookups ---"
        def slurper = new JsonSlurper();
        def processingClosure = {String query, OutputStream os ->
            def searchResults = tdb.search(slurper.parseText(query));
            def sz = searchResults.length
            System.err.println("SEARCH RESULT: ${searchResults}")
            os.write("{\"count\":${sz},\"data\":[".getBytes('UTF-8'))
            searchResults.eachWithIndex{ n, idx ->
                long targetOffset = offsetIndex[n]
                System.err.println "\nFetching line ${n} from offset ${targetOffset}..."
                def data = readLineFromOffset(globalMemMap, targetOffset);
                //System.err.println "Result: \"${data}\"\n"
                os.write(data.getBytes("UTF-8"))
                if(idx + 1 < sz) {os.write(44)}; os.write(10)
            }
            os.write("]}".getBytes('UTF-8'))
        }

        [
            '{"and":["name:sepol*", "shortName:bl*sepol*"]}',
            '{"or":["chain:AC?", "chain:Ab?y*"]}',
            '{"or":["chain:Ab?y*", {"and":["name:sepol*", "shortName:bl*sepol*"]}]}'
        ].each { s ->
            processingClosure(s, erro)
        }

/*
        index.values().toList().shuffled().take(3).each {lst ->
            lst.each{ n ->
            long targetOffset = offsetIndex[n]
            erro.println "Fetching line ${n} from offset ${targetOffset}..."
            erro.println "Result: \"${readLineFromOffset(globalMemMap, targetOffset)}\"\n"
            }
        }
*/
        HttpSvr svr = new HttpSvr(WEB_PORT.toInteger(), processingClosure);
        erro.println("Starting HTTP sever")
        svr.start();
/*
        while(true){
            try{Thread.sleep(Long.MAX_VALUE);}
            catch(Exception e){erro.println("Exiting server thread"); break}
        }
*/
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

*/
