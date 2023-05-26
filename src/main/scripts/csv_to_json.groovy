import groovy.json.*;
import java.util.*;
import static com.entity.groovy.utils.*;

BATCH_SIZE = 10000;//30000;
DEFAULT_SEP = ',';

field_map = [ : ];
separator = ',';

println( 'ARGS ' + args );
println( 'NAMED ARGS ' + named_args);

if( args.length < 3 )
{
    println( "Usage: ${args[ 0 ]} config_file input_csv_file output_json_file [skiphdr=[y|n]" );
    println( "config file is XM file in format of java properties file" );
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
    println( "\ncsv_to_json.groovy csv_to_json.xml skiphdr=y cities.txt cities.json\n" );
    System.exit( -1 );
}

def props = getPropertiesMap( args[ 1 ], LOAD_XML );

println( props );
process_file( props, args[ 2 ], args[ 3 ] );

//-----------------------------------------------------------------------------
def process_file( props, in_file, out_file )
{
    def jsonSlurper = new JsonSlurper();
    def trim_flag = 'yes' == props['trim'],
        sep = props[ 'fsep' ] ? props[ 'fsep' ] : DEFAULT_SEP,
        field_str = props['use.fields'] && props['use.fields'].trim() ? props['use.fields'].trim() : '',
        selected_field_str = props['selected.fields'] && props['selected.fields'].trim() ? props['selected.fields'].trim() : '',
        skip_header = ['yes', 'y' ].contains( named_args[ 'skiphdr' ] ),
        use_header = ['yes', 'y' ].contains( props['use.header']  ) || ! field_str,
        el = [],
        fm = null; /* field map in format where k = field ordinal, v = field name*/

    def f = new File( in_file );

    if( ! f.exists() )
    {
        println "${in_file} does not exist";
        return;
    }

    def o = new File( out_file );

    if( o.exists() )
      { o.delete(); }
    o.createNewFile();

    if( sep == '|' )
      { sep = '\\|'; }

    f.eachLine
    { line, line_number ->

        if( line_number % 100 == 0 )
          { print( "\rline count: " + line_number ); }

        def l = line.trim();

        if( line_number == 1 )
        {
            fm = parse_header( line, field_str, selected_field_str, sep, use_header );
        }

        if( l.size() > 0 && ( ! skip_header || line_number > 1 ) )
        {
            //println "\n==> " + l;

            def ss = sep != ',' ? Arrays.asList( l.split( sep, -1 ) ) : split_csv_record( l ),
                sz = ss.size(),
                e = jsonSlurper.parseText( '{}' );

            fm.each
            { k, v ->

                if( k < sz )
                {
                    // see if we have list in our field value, most likely not
                    //
                    def z = expand( jsonSlurper, ss[ k ] );
                    if( z instanceof String )
                      { z = trim_flag ? z.replace('"','').trim() : z.replace('"',''); }
                    //else
                    //  { println( "THIS IS LIST " + z ); }
                    e[ v ] = z;
                }
                else
                { e[ v ] = ''; }
            }
            el.add( e );
        }

        if( el.size() >= BATCH_SIZE )
        {
            to_file( o, el );
            el = [];
        }
    }

    to_file( o, el );

    return el;
}
//-----------------------------------------------------------------------------
def expand( parser, val )
{
    def ret = null;

    try
    {
        if( ! val.trim().startsWith( "[" ) )
          { return val; }
        ret = parser.parseText( val )
        if( ret instanceof List && ret.size() == 1 && ret[0] == "" )
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
def to_file( out_file, data_list )
{
    if( ! ( data_list.size() > 0 ) )
      { return; }

    for( z in data_list )
    {
        out_file.withWriterAppend('UTF-8')
        { writer ->
            writer.write( JsonOutput.toJson( z ) + '\n' )
        }

    }
}
//-----------------------------------------------------------------------------
def parse_header( header, field_str, selected_field_str, sep, use_header )
{
    def a;
    def s = sep == '|' ? '\\|' : sep;

    a = use_header ? a = header.toLowerCase().split( s, -1) : field_str.split( '\\|' );

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
}
