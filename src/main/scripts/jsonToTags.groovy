#!/usr/bin/env groovy

import groovy.json.*;
import java.util.regex.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.time.*;

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
    private static final ForkJoinPool pool = new ForkJoinPool(Runtime.runtime.availableProcessors())

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
        return searchHelperMultiThreaded(query, keys, index);
    }

    private static int [] searchHelperMultiThreaded(Map query, String [] keys, Map<String, intArray> index)
    {
        int [] result = null;

        query.each { k, v ->
            // Separate items into maps (recursive) and strings (fuzzy search)
            def subQueries = v.findAll { it instanceof Map }
            def searchStrings = v.findAll { it instanceof String }

            System.err.printf("\nSUBQUERIES: ${subQueries}\n")
            System.err.printf("SEARCHES: ${searchStrings}\n")

            // 1. Run recursive sub-queries directly on the CURRENT thread (No future overhead)
            subQueries.each { item ->
                int[] subResult = searchHelperMultiThreaded(item, keys, index)
                result = setop(k, result, subResult)
            }

            // 2. Parallelize the heavy string fuzzy searches using Java Parallel Streams
            if (!searchStrings.isEmpty()) {
                // Run inside custom ForkJoinPool to control parallelism
                List<int[]> parallelResults = pool.submit({ ->
                    searchStrings.parallelStream().map { item ->
                        def tmp = []
                        fuzzySearch(keys, item.toLowerCase()).each { key ->
                            tmp.addAll(index[key].offs)
                        }
                        // Sort inside the parallel thread
                        tmp.sort()
                        return tmp as int[]
                    }.collect(Collectors.toList())
                }).get()

                // 3. Merge the parallel results back sequentially
                parallelResults.each { taskResult ->
                    result = setop(k, result, taskResult)
                }
            }
        }

        return result
    }

    private static int [] searchHelper(Map query, String [] keys, Map<String, intArray> index){

        long startTime = System.currentTimeMillis()
        int [] result;
        query.each {k, v ->
            System.err.println("${k} -> ${v}")
            v.each { item ->
                if(item instanceof Map){
                    System.err.println("ITEM: ${item}")
                    result = setop(k, result, searchHelper(item, keys, index))
                } else if(item instanceof String) {
                    System.err.println("SEARCHING FOR:${item}")
                    def tmp = [];
                    fuzzySearch(keys, item.toLowerCase()).each { key ->
                        //System.err.println("-> ${key}")
                        tmp.addAll(index[key].offs)
                        //System.err.println "\nTMP: ${tmp}"
                    }
                    System.err.println( "fuzzy.search complete in ${System.currentTimeMillis() - startTime}")
                    tmp.sort()
                    //System.err.println "\nTMP: ${tmp}"
                    result = setop(k, result, tmp as int[])
                }
            }

        }
        //System.err.println("!!! RESULT: ${result}")
        return result;
    }

    private static int[] setop(String op, int[] arr1, int[] arr2){
        System.err.println("...applying ${op}");
        switch(op){
            case "and": return findIntersectionInPlace(arr1, arr2); //findIntersection(arr1, arr2); //
            case "or" : return findUnion(arr1, arr2);
            default:
                return findUnion(arr1, arr2);
        }
    }

    public static int[] findIntersection(int[] arr1, int[] arr2) {
        //System.err.println("1. FIND INTERSECTION: ${arr1} ${arr2}")
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
        //System.err.println("2. FIND INTERSECTION: ${temp} ${k}")
        // 3. Trim the temporary array to the actual number of matched elements
        return Arrays.copyOf(temp, k);
    }

    public static int[] findIntersectionInPlace(int[] arr1, int[] arr2) {
        if(arr1 == null){ return arr2;}
        if(arr2 == null){ return arr1;}

        int i = 0, j = 0, k = 0;

        // Traverse arrays together using two pointers
        while (i < arr1.length && j < arr2.length) {
            if (arr1[i] < arr2[j]) {
                i++;
            } else if (arr1[i] > arr2[j]) {
                j++;
            } else {
                // Found a match. Skip duplicates.
                // Check against our in-place boundary 'k' inside arr1
                if (k == 0 || arr1[k - 1] != arr1[i]) {
                    arr1[k++] = arr1[i]; // Overwrite arr1 in-place
                }
                i++;
                j++;
            }
        }
        // System.err.println("2. FIND INTERSECTION IN PLACE: ${arr1} ${k}")
        // Trim and return only the intersection slice of arr1
        return Arrays.copyOf(arr1, k);
    }

    public static int[] findUnion(int[] arr1, int[] arr2) {
        //System.err.println("1. FIND UNION: ${arr1} ${arr2}")
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
        //System.err.println("2. FIND UNION: ${temp} ${k}")
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
                    try
                    {
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

                        // 4. Write the response body stream safely using withStream
                        if(onPost){
                            try{


                            new JsonSlurper().parseText(requestBody)
                            // CRITICAL CHANGE: Send the headers ONLY when ready to write the body
                            exchange.sendResponseHeaders(200, 0)
                            exchange.responseBody.withStream { outStream -> onPost(requestBody, outStream) }
                            } catch(IllegalArgumentException | JsonException ea){
                                System.err.println("@@@ Bad Request Error: ${ea.message}")
                                sendErrorResponse(exchange, 400, "Bad request: ${ea.message}")
                            } catch(Throwable e){
                                System.err.println("@@@@@@ Internal Server Error occurred:")
                                org.codehaus.groovy.runtime.StackTraceUtils.sanitize(e).printStackTrace(System.err)
                                sendErrorResponse(exchange, 500, "Internal Server Error: ${e.message}")
                            }
                        } else {
                            // Send explicit content length instead of chunked (0) when possible
                            exchange.sendResponseHeaders(200, responseBytes.length)
                            exchange.responseBody.withStream { outStream -> outStream.write(responseText.getBytes("UTF-8"))}
                        }
                    } catch(Throwable e){
                        System.err.println("!!!!!!!!!! Internal Server Error occurred:")
                        org.codehaus.groovy.runtime.StackTraceUtils.sanitize(e).printStackTrace(System.err)
                        //e.printStackTrace(System.err)
                        sendErrorResponse(exchange, 500, "Internal Server Error: ${e.message}")

                        throw e;
                    }
                } else {
                    // Method Not Allowed
                    sendErrorResponse(exchange, 405, "Method not allowed. POST only.")
                }
            }
        }

        // 2. Register the JVM Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread({
           shutdown();
        }))
    }

    private void sendErrorResponse(com.sun.net.httpserver.HttpExchange exchange, int statusCode, String description) {
        try {
            // 1. Convert description to a safe byte array (GStringImpl friendly)
            byte[] responseBytes = description.toString().getBytes("UTF-8")

            // 2. Set the Content-Type header so the client knows it's text
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8")

            // 3. Send the status code along with the fixed body length
            exchange.sendResponseHeaders(statusCode, responseBytes.length)

            // 4. Stream the actual description text out
            exchange.getResponseBody().withStream { outStream ->
                outStream.write(responseBytes)
            }
        } catch (Exception fatal) {
            System.err.println("Fatal: Could not transmit error status down socket: ${fatal.message}")
        }
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

static String msformatted(long ms)
{
    // Create a local time starting at midnight and add the milliseconds
    LocalTime time = LocalTime.MIDNIGHT.plus(Duration.ofMillis(ms))
    // Format it exactly as MM:ss.SSS
    return time.format(java.time.format.DateTimeFormatter.ofPattern("mm:ss.SSS"))
}

void process()
{
    if( !(this.binding.variables['named_args'] && this.binding.variables['named_args'].size() > 0))
    {
        demo(); return;
    }

    boolean printData = named_args['print'] && named_args['print'] == "true"
    def fileName = named_args['file'];
    def port = named_args['port'] && named_args['port'].isNumber() ? named_args['port'].toInteger() : WEB_PORT.toInteger()
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

        long startTime = System.currentTimeMillis()
        List<Long> offsetIndex = buildLineOffsetIndex(globalMemMap)
        erro.println "Index generation complete in ${System.currentTimeMillis() - startTime} ms."
        erro.println "Total lines indexed: ${offsetIndex.size()}\n"

        // 4. Instant point-lookups (O(1) complexity lookup)
        def processingClosure = {String query, OutputStream os ->
            try{
                startTime = System.currentTimeMillis()
                def searchResults = tdb.search(new JsonSlurper().parseText(query));
                erro.println "tdb.search complete in ${msformatted(System.currentTimeMillis() - startTime)}"

                def sz = searchResults.length
                //erro.println("SEARCH RESULT: ${searchResults}")
                os.write("{\"data\":[".getBytes('UTF-8'))
                startTime = System.currentTimeMillis();
                searchResults.eachWithIndex{ n, idx ->
                    long targetOffset = offsetIndex[n]
                    //erro.println "\nFetching line ${n} from offset ${targetOffset}..."
                    def data = readLineFromOffset(globalMemMap, targetOffset);
                    //System.err.println "Result: \"${data}\"\n"
                    os.write(data.getBytes("UTF-8"))
                    if(idx + 1 < sz) {os.write(44)}; os.write(10)
                }
                erro.println "mem map records retrieval complete in ${msformatted(System.currentTimeMillis() - startTime)}"
                os.write("],\n\"count\":${sz}}".getBytes('UTF-8'))
            }catch(Throwable e){
                System.err.printf("\n!!! in processingClosure: %s\n", e.getMessage())
                e.printStackTrace(System.err);
                throw e;
            }
        }
/*
        [
            '{"and":["name:sepol*", "shortName:bl*sepol*"]}'
        ].each { s ->
            processingClosure(s, erro)
        }
*/
/*
        index.values().toList().shuffled().take(3).each {lst ->
            lst.each{ n ->
            long targetOffset = offsetIndex[n]
            erro.println "Fetching line ${n} from offset ${targetOffset}..."
            erro.println "Result: \"${readLineFromOffset(globalMemMap, targetOffset)}\"\n"
            }
        }
*/
        HttpSvr svr = new HttpSvr(port, processingClosure);
        erro.println("Starting HTTP sever on port ${port}")
        svr.start();
/*
        while(true){
            try{Thread.sleep(Long.MAX_VALUE);}
            catch(Exception e){erro.println("Exiting server thread"); break}
        }
*/
    } catch(Exception e){
        System.err.println("EXCEPTION: ${e.getMessage()}")
        e.printStackTrace();
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
cls; gi.sh  ~/work/groovy/jsonToTags.groovy file=/home/oo/ephemeral/ev.json print=true port=9091 > ~/ephemeral/dump1.txt
*/

/* queries

{"or":["chain:Ab?y*", "name:ze*", {"and":["name:so*", "name:can*"]}]}
{"or":["chain:AC?", "chain:Ab?y*"]}',
{"or":["chain:Ab?y*", {"and":["name:sepol*", "shortName:bl*sepol*"]}]}'
{"and":["vin:*135*",{"and": ["city:*e*", "city:*s*",{"or":["utility:*nn*", "utility:*ge*"]}]}, "postal-code:98*"] }
*/