import groovy.json.*;

import static com.entity.groovy.utils.*;
import com.entity.indexing.Indexer;
import com.entity.util.*;

BATCH_SIZE = 30000;
DEFAULT_SEP = ',';

println( 'ARGS ' + args );
println( 'NAMED ARGS ' + named_args);
println( 'BATCH SIZE ' + BATCH_SIZE);

if( args.length < 3 || named_args.size() < 1 )
{
    println( "Usage: ${args[ 0 ]} schema_file=path_to_xml_schema input_json_file output_index_folder " +
             "mode=[create|append] {id_field=no_spaces_id_field_name" );
    println( "config file is XML file in format of java properties file" );
    println( "Example: schema.xml\n" +
    '''
    <table>
    <field name="first_name" index="true" type="text" store="true"/>
    <field name="id" index="true" type="string" store="true"/>
    </table>
    ''' );
    println( "\n${args[0]} schema_file=schema.xml entities.json index_dir mode=create id_field=ssn\n" );
    println( '\nif your file has a field in following format ["val1","val2",...,"valN"]\nit will be indexed as array ( double quotes are important)' );
    return;
}

def schema = StringUtils.isNotBlank( named_args[ 'schema_file' ] ) ?
             process_schema( named_args[ 'schema_file' ] ) : null;

def id_field = named_args[ 'id_field' ];

println( "SCHEMA " + schema );
process_file( schema, args[ 1 ], args[ 2 ], id_field, ( named_args[ 'mode' ] == 'create' ) );

//-----------------------------------------------------------------------------
def process_file( schema, in_file, idx_dir, id_field, create )
{
  println("in_file ${in_file} idx_dir ${idx_dir}")
    def jsonSlurper = new JsonSlurper();
    def el = [],
        fm = null;

    def f = new File( in_file );

    if( ! f.exists() )
    {
        println "${in_file} does not exist";
        return;
    }

    def l = '',
        idxr = new Indexer( idx_dir, null, create );

    f.eachLine
    { line, line_number ->

        if( line_number % 100 == 0 )
          { System.err.printf( "\rline count: %d", line_number ); }

        l = line.trim();


        if( l.size() > 0 )
        {
            //println "\n==> " + l;

            def e = jsonSlurper.parseText( l );

            el.add( e );

            if( ! schema )
              { schema = create_default_schema( e ); }
        }
/**/
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
def to_index( data_list, idxr, schema, id_field, create )
{
    def doc = null;
    data_list.each
    { e ->
        e._id = UUID.randomUUID().toString()
        schema.each
        { field_name, m ->
            def val = e[ field_name ];
            //println( "\n${field_name} ${m} " );
//println( "\n${field_name} ${val} " );
            if( m[ 'raw' ] == 'true' )
              { val = JsonEscapeUtils.escape( JsonOutput.toJson( e ) ); }/**/
            else
            if( val == null )
              { return; }

            if( val instanceof List )
              { doc = add_list_to_doc( idxr, doc, schema, field_name, val ); }
            else
            if( val instanceof Map )
              { doc = add_map_to_doc( idxr, doc, schema, field_name, val ); }
            else
              { doc = idxr.addToDoc( doc, schema, field_name, val ); }
        }
/*
        e.each
        { k, v ->

            //println( "\n${k} ${v} " );
            println( "\n${k} " + schema[ k ][ 'raw' ] );

            if( ( v instanceof List ) )
              { doc = add_list_to_doc( idxr, doc, schema, k, v ); }
            else
            if( v instanceof Map )
              { doc = add_map_to_doc( idxr, doc, schema, k, v ); }
            else
              { doc = idxr.addToDoc( doc, schema, k, v ); }
        }
*/
        if( ! create && StringUtils.isNotBlank( id_field  ) )
          { idxr.indexDoc( doc, id_field, e[ id_field ] ); }
        else
          { idxr.indexDoc( doc ); }

        doc.clear();
    }
}
//-----------------------------------------------------------------------------
def add_map_to_doc( idxr, doc, schema, field, map )
{
    if( ! map )
      { return doc; }

    map.each
    { k, v ->
        //println( "${k} -> ${v}\n" );
        doc = idxr.addToDoc( doc, schema, field, k + '@' + ( !( v instanceof Map ) ?
                                                              StringUtils.join( v, ' ' ) : v.toString() ) );
    }
    return doc;
}
//-----------------------------------------------------------------------------
def add_list_to_doc( idxr, doc, schema, field, lst )
{
    if( ! lst )
      { return doc; }

    lst.toSet().each
    { i ->
        //println( "\n!!! ADDING LIST FIELD " + field );
        //println( "\n!!! " + i );
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
def create_default_schema( e )
{
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
