import groovy.json.*;
import org.json.*;
import java.util.*;
import com.entity.indexing.Indexer;
import com.entity.util.*;
import static com.entity.groovy.utils.*;

BATCH_SIZE = 100000;
DEFAULT_SEP = ',';

field_map = [ : ];
separator = ',';

println( 'ARGS ' + args );
println( 'NAMED ARGS ' + named_args);

if( args.length < 3 )
{
    println( "Usage: ${args[ 0 ]} config_file input_csv_file output_index_dir [skiphdr=y|n] [mode=create|append] [id_field=name of id field] [schema_file=path to xml schema]" );
    println( "config file is XML file in format of java properties file" );
    println( "Example:\n" +
    '''
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
    <properties>
    <!-- use.header yes|no . use header to assign field names ... -->
    <entry key="use.header">yes</entry>

    <!-- ... or define field names here as pipe separated list -->
    <entry key="use.fields">country|city|accentcity|region|population|latitude|longitude</entry>

    <!-- use only following fields either by name or by index -->
    <entry key="selected.fields">1|latitude|longitude</entry>

    <!-- trim column content -->
    <entry key="trim">true</entry>

    <!-- field separator could be ommitted, default comma -->
    <entry key="fsep">,</entry>

    <!-- record separator could be ommitted, default new line -->
    <entry key="rsep">\n</entry>

    </properties>
    ''' );
    println( "\n${args[ 0 ]} csv_to_json.xml skiphdr=y cities.txt /dbdata/index/cities mode=create schema_file=cities.xml\n" );
    println( "\n${args[ 0 ]} csv_to_json.xml skiphdr=y cities.txt /dbdata/index/cities mode=append schema_file=cities.xml id_field=city_id\n" );
    println( "\n${args[ 0 ]} skiphdr=y mode=create schema_file=cities_schema.xml cities_csv_to_json.xml worldcitiespop.txt /dbdata/index/cities\n" );
    System.exit( -1 );
}

def schema = StringUtils.isNotBlank( named_args[ 'schema_file' ] ) ?
             process_schema( named_args[ 'schema_file' ] ) : null;

def id_field = named_args[ 'id_field' ];

println( "SCHEMA " + schema );

def props = getPropertiesMap( args[ 1 ], LOAD_XML );

println( props );
process_file( schema, props, args[ 2 ], args[ 3 ], id_field, ( named_args[ 'mode' ] == 'create' ) );

//-----------------------------------------------------------------------------
def process_file( schema, props, in_file, idx_dir, id_field, create )
{
    def trim_flag = 'yes' == props['trim'],
        sep = props[ 'fsep' ] ? props[ 'fsep' ] : DEFAULT_SEP,
        field_str = props['use.fields'] && props['use.fields'].trim() ? props['use.fields'].trim() : '',
        selected_field_str = props['selected.fields'] && props['selected.fields'].trim() ? props['selected.fields'].trim() : '',
        skip_header = ['yes', 'y' ].contains( named_args[ 'skiphdr' ] ),
        use_header = ['yes', 'y' ].contains( props['use.header']  ) || ! field_str,
        el = [],
        fm = null;

    def idxr = new Indexer( idx_dir, null, create );
    def f = new File( in_file );

    if( ! f.exists() )
    {
        println "${in_file} does not exist";
        return;
    }


    if( sep == '|' )
      { sep = '\\|'; }

    def l;

    f.eachLine
    { line, line_number ->

        if( line_number % 100 == 0 )
          { print( "\rline count: " + line_number ); }

        l = line.trim();

        if( line_number == 1 )
        {
            fm = parse_header( line, field_str, selected_field_str, sep, use_header );
            println( "FIELD MAP: ${fm}" );
        }

        if( l.size() > 0 && ( ! skip_header || line_number > 1 ) )
        {
            //println "\n==> " + l;

            def ss = sep != ',' ? Arrays.asList( l.split( sep, -1 ) ) : split_csv_record( l ),
                sz = ss.size(),
                e = new JSONObject(); //jsonSlurper.parseText( '{}' );

            fm.each
            { k, v ->

                if( k < sz )
                {
                    // see if we have list in our field value, most likely not
                    //
                    def z = expand( ss[ k ] );
                    if( z instanceof String )
                      { z = trim_flag ? z.replace('"','').trim() : z.replace('"',''); }
                    //else
                    //  { println( "THIS IS LIST " + z ); }
                    e.put( v , z );
                }
                else
                { e.put( v, ''); }
            }

            if( ! schema )
              { schema = create_default_schema( e ); }

            el.add( e );
        }

        if( el.size() >= BATCH_SIZE )
        {
            System.err.printf( "\rline count: %d indexing batch...", line_number );

            try
            {
                to_index( el, idxr, schema, id_field, create );
            }
            catch( ex )
            {
                println( "\nException on line " + l + "!!!\n" + ex );
                ex.printStackTrace();
            }
            el = [];
        }

    }

    System.err.printf( "\nindexing last batch of %d", el.size() );
    try
    {
        to_index( el, idxr, schema, id_field, create );
    }
    catch( e )
    {
        println( "Exception on line " + l + "\n" + e );
        e.printStackTrace();
    }

    idxr.closeIndexWriter();

    return el;
}
//-----------------------------------------------------------------------------
def expand( /*parser,*/ val )
{
    def ret = null;

    try
    {
        if( ! val.trim().startsWith( "[" ) )
          { return val; }
        ret = new JSONArray(val);//parser.parseText( val )
        if( ret instanceof JSONArray && ret.size() == 1 && ret.get(0) == "" )
          { ret = []; }
    }
    catch( e )
    {
        ret = val;
    }
    return ret;
}
//----------------------------------------------------------------------------
def split_csv_record( record )
{
   def match = record =~ /(?si)([^,"]*(?:".*?")*[^,"]*|[^,]*)(?:,|$)/;
   def fields = [] as List;

   while( match.find() )
   {
      fields.add( match.group( 1 ).replace('"','').replace( '\\n', "," ) );
   }

   return fields;
}
//-----------------------------------------------------------------------------
def parse_header( header, field_str, selected_field_str, sep, use_header )
{
    def a;
    def s = sep == '|' ? '\\|' : sep;

    a = use_header ? header.toLowerCase().split( s, -1) : field_str.split( '\\|' );

    a.eachWithIndex
    {
        e, idx -> field_map[ idx ] = e;
    }

    if( ! selected_field_str )
      { return field_map; }

    a = selected_field_str.toLowerCase().replaceAll( '(\r|\n|\\s+)', '').split( '\\|' );
    def vals = field_map.values();
        fm = [:];

    def extra_fields = [] as Set;

    a.each
    { v -> //println( "--> " + v );
        if( v.isInteger() && field_map[ v.toInteger() ] )
        {
            v = v.toInteger();
            fm[ v ] = field_map[ v ];
        }
        else
        if( vals.contains( v ) )
        {
            for( k in field_map.keySet() )
            {
                if( field_map[ k ] == v )
                {
                    fm[ k ] = v; break;
                }
            }
        }
        else
        {
            extra_fields.add( v );
        }
    }

    def max_key = 0;
    for( k in fm.keySet() )
    {
        if( k > max_key )
          { max_key = k; }
    }

    max_key += 1;

    extra_fields.each
    { f ->
        fm[ max_key++ ] = f;
    }
    //println( "EXTRA FIELDS " + extra_fields );
    //println( fm );
    return fm;
}//-----------------------------------------------------------------------------
def create_default_schema( e )
{println( "!!!!! CREATING DEFAULT SCHEMA" );
    def m = [ 'index':'true', 'store':'true', 'type':'text'],
        mm = [:];

    e.each
    {  k,v ->
        mm[ k ] = m;
    }
    mm[ 'content' ] = [ 'index':'true', 'store':'true', 'raw':'true', 'type':'symbol'];
    println( mm );
    return mm;
}
//-----------------------------------------------------------------------------
def add_list_to_doc( idxr, doc, schema, field, lst )
{
    if( ! lst )
      { return doc; }

    lst.each
    { i ->
        try
        {
            doc = idxr.addToDoc( doc, schema, field, i );
        }
        catch( Exception e )
        {
            println( e );
            println( "FAILED FIELD ${field} VALUE ${val}" );
        }

    }
    return doc;
}
//-----------------------------------------------------------------------------
def to_index( data_list, idxr, schema, id_field, create )
{
    def doc = null;
    data_list.each
    { e ->
//println( e.toString(2) );
        schema.each
        { field_name, m ->
            def val = e.opt( field_name );
            //println( "\n${field_name} ${m} " );
//println( "\n${field_name} ${val} " );
            if( m[ 'raw' ] == 'true' )
              { val = JsonEscapeUtils.escape( e.toString() ); }
            else
            if( val == null )
              { return; }

            if( val instanceof JSONArray )
              { doc = add_list_to_doc( idxr, doc, schema, field_name, val ); }
            else
              { doc = idxr.addToDoc( doc, schema, field_name, val ); }
        }

        if( ! create && StringUtils.isNotBlank( id_field  ) )
          { idxr.indexDoc( doc, id_field, e[ id_field ] ); }
        else
          { idxr.indexDoc( doc ); }

        doc.clear();
    }
}
