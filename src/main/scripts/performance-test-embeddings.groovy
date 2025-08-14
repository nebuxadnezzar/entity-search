import groovy.json.*;
import com.entity.util.*;
import java.util.concurrent.*;


ROUNDS = 500;
DRAIN_TO = 8000;
POOL_SZ = 8;
url_str = 'http://localhost:8888/';
query_str = '{"and": [{"or":["name:m?sta*a"]}, {"or":["aliases:muhammed", "aliases:abu"]}]}'
query_str = '{"and": [{"or":["name:mustafa","name:mostafa"]}, {"or":["aliases:muhammed", "aliases:abu"]}]}'
process();

def process()
{
    def results = Collections.synchronizedList([])
    def pool = Executors.newFixedThreadPool(POOL_SZ, new SimpleUtils.DaemonThreadFactory())
    def d1 = System.currentTimeMillis();
    (1..ROUNDS).each{
        n ->
        System.out.print("\rsearch # ${n} ")
        run_helper(query_str, pool, results)
    }
    
    println("\n${ROUNDS} searches submitted  in ${DateDiffUtils.diffToString(d1, System.currentTimeMillis())} sec.")
    shutdown( pool, results)
    println("Searches completed  in ${DateDiffUtils.diffToString(d1, System.currentTimeMillis())} sec.")
}
//-----------------------------------------------------------------------------
def run_helper(data, pool, results)
{//println(" DATA SIZE: ${data ? data.size() : 'data is null'} ")
    //println("Running ${data.size()} records for ${match_type}")
    def thread = {c -> pool.submit(c as Callable)};
    results.add( thread{run_match(data)});
}
//-----------------------------------------------------------------------------
def run_match(data)
{
    def resp = send_request(url_str, data)
    //println("SCORE: ${resp.score}")
    return resp.score
}


//-----------------------------------------------------------------------------
def get_response_output(http_con)
{
    def sb = new StringBuilder();
    http_con.getInputStream().eachLine
    { line ->
        //println("${line}")
        sb.append(line);
    }
    return sb.toString()
}
//-----------------------------------------------------------------------------
def send_request(urlstr, data)
{
    def bytes = data.getBytes('UTF-8');
    //println("URL: " + "${urlstr}${match_type}")
    URL url = new URL(urlstr);
    HttpURLConnection con = (HttpURLConnection)url.openConnection();
    con.setDoOutput(true);
    con.setRequestMethod("POST");
    con.setRequestProperty('Content-Type', 'application/json');
    con.setRequestProperty('Content-length', String.valueOf(bytes.length));
    def out = con.getOutputStream();
    out.write(bytes);

    def rs_data = new JsonSlurper().parseText(get_response_output(con));
    //println("\n${rs_data}")
    return rs_data;
}
 //----------------------------------------------------------------------------
    def drain_results( results )
    {
       results.each
       { result ->
          try
          {
             //println("${result}")
             def v = result.get( DRAIN_TO, TimeUnit.SECONDS );
             //println("${v}")
             if( result.isCancelled() )
               { System.err.println( "TASK ${result} timed out" ); }
          }
          catch( Exception e )
          {
             System.err.println( e.getMessage() );
             result.cancel( true );
          }
       };
    }
//----------------------------------------------------------------------------
def shutdown( pool, results )
{
    drain_results( results );
    pool.shutdown();
    pool.awaitTermination( 60, TimeUnit.SECONDS);
}
