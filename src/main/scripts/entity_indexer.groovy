
import groovy.json.*;

import static com.entity.groovy.utils.*;
import com.entity.indexing.Indexer;
import com.entity.util.*;
import java.util.UUID;

BATCH_SIZE = 30000;
DEFAULT_SEP = ',';
TBL_KEY = '__table__';

println( 'ARGS ' + args );
println( 'NAMED ARGS ' + named_args);
println( 'BATCH SIZE ' + BATCH_SIZE);

if( args.length < 4 || named_args.size() < 1 )
{
    println( "Usage: ${args[ 0 ]} schema_file input_json_file output_index_folder " +
             "mode=[create|append] {index_name=no_spaces_name} {scrape_name=no_spaces_name}" );
    println( "config file is XML file in format of java properties file" );
    println( "Example:\n" +
    '''
    <table name="mytable">
    <field name="first_name" index="true" type="text" store="true"/>
    <field name="id" index="true" type="string" store="true"/>
    </table>
    ''' );
    println( "\n${args[0]} schema.xml entities.json index_dir mode=create index_name=test\n" );
    return;
}

def schema = process_schema( args[ 1 ] );

if( named_args[ 'index_name' ] )
  { schema[ TBL_KEY ] = named_args[ 'index_name' ]; }

println( schema );
process_file( schema, args[ 2 ], args[ 3 ], named_args[ 'scrape_name' ], ( named_args[ 'mode' ] == 'create' ? true : false ) );

//-----------------------------------------------------------------------------
def process_file( schema, in_file, idx_dir, scrape_name, create )
{
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
        tbl_name = schema[ TBL_KEY ],
        idxr = new Indexer( idx_dir + ( tbl_name ? System.getProperty( 'file.separator' ) + tbl_name  : '' ), null, create );

    f.eachLine
    { line, line_number ->

        if( line_number % 10 == 0 )
          { System.err.printf( "\rline count: %d", line_number ); }

        l = line.trim();


        if( l.size() > 0 )
        {
            //println "\n==> " + l;

            def e = jsonSlurper.parseText( l );

            el.add( e );
        }

        if( el.size() >= BATCH_SIZE )
        {
            System.err.printf( "\rline count: %d indexing batch...", line_number );

            try
            {
                to_index( el, idxr, schema, scrape_name, create );
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
        to_index( el, idxr, schema, scrape_name, create );
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
def to_index( data_list, idxr, schema, scrape_name, create )
{
    def doc = null;
    data_list.eachWithIndex
    { e, idx ->
        e._id = UUID.randomUUID().toString()
        doc = idxr.addToDoc( doc, schema, 'type', e.type );
        doc = idxr.addToDoc( doc, schema, 'entity_id', e.entity_id );
        doc = idxr.addToDoc( doc, schema, 'data_source_id', e.data_source_id );
        doc = idxr.addToDoc( doc, schema, 'soundex', e.soundex );
        doc = idxr.addToDoc( doc, schema, 'stripped_name', e.stripped_name );
        doc = idxr.addToDoc( doc, schema, 'name', e.name.trim() );
        doc = idxr.addToDoc( doc, schema, 'scrape_name', ( ! scrape_name && e.scrape_name ? e.scrape_name.trim() : scrape_name ) );
        doc = add_list_to_doc( idxr, doc, schema, 'remarks', e.remarks );
        doc = add_list_to_doc( idxr, doc, schema, 'jurisdictions', e.jurisdictions );
        doc = add_list_to_doc( idxr, doc, schema, 'industries', e.industries );
        doc = add_list_to_doc( idxr, doc, schema, 'industry_codes', e.industry_codes );
        doc = add_map_to_doc( idxr, doc, schema, 'positions', e.misc_info.positions );
        doc = add_list_to_doc( idxr, doc, schema, 'employeeOf', e.associations.employeeOf );
        doc = add_list_to_doc( idxr, doc, schema, 'childOf', e.associations.childOf );
        doc = add_list_to_doc( idxr, doc, schema, 'partnerOf', e.associations.partnerOf );
        doc = add_list_to_doc( idxr, doc, schema, 'studentOf', e.associations.studentOf );
        doc = add_list_to_doc( idxr, doc, schema, 'participantOf', e.associations.participantOf );
        doc = add_list_to_doc( idxr, doc, schema, 'aliases', e.aliases );
        doc = idxr.addToDoc( doc, schema, 'content', JsonEscapeUtils.escape( JsonOutput.toJson( e ) ) );
        //println( JsonEscapeUtils.escape( JsonOutput.toJson( e ) ) );
        //println( doc );
        if( ! create && StringUtils.isNotBlank( e.entity_id  ) )
          { idxr.indexDoc( doc, 'entity_id', e.entity_id ); }
        else
          { idxr.indexDoc( doc ); }
        /*
        doc.removeFields( 'type' ); doc.removeFields( 'entity_id' );
        doc.removeFields( 'data_source_id' ); doc.removeFields( 'soundex' );
        doc.removeFields( 'stripped_name' ); doc.removeFields( 'name' );
        doc.removeFields( 'scrape_name' ); doc.removeFields( 'remarks' );
        doc.removeFields( 'jurisdictions' ); doc.removeFields( 'industries' );
        doc.removeFields( 'industry_codes' ); doc.removeFields( 'positions' );
        doc.removeFields( 'employeeOf' ); doc.removeFields( 'childOf' );
        doc.removeFields( 'partnerOf' ); doc.removeFields( 'studentOf' );
        doc.removeFields( 'participantOf' ); doc.removeFields( 'content' );
        */
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
        doc = idxr.addToDoc( doc, schema, field, k + '@' + StringUtils.join( v, ' ' ) );
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
        //println( "\n!!! ADDING FIELD " + field );
        //println( "\n!!! " + i );
        doc = idxr.addToDoc( doc, schema, field, i );
    }
    return doc;
}
//-----------------------------------------------------------------------------
def create_content( e )
{
    def buf = [] as Set;

    buf.add( e.type );
    buf.add( e.entity_id );
    buf.add( e.data_source_id  );
    buf.add( e.soundex );
    buf.add( e.stripped_name );
    buf.add( e.name );
    buf.add( e.aliases );
    buf.add( e.jurisdictions );
    buf.add( StringUtils.join( e.citizenships, ' ' ) );
    buf.add( e.addresses.toString() );

    return StringUtils.join( buf, ' ' );


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

/*
entity JSON
    {
        "entity_id":"",
        "soundex": "",
        "stripped_name": "",
        "name": "",
        "type": "O",
        "subtype":"",
        "data_source_id": "",
        "active": "true",
        "risk_score":"{}",
        "addresses": [],
        "aliases": [],
        "associations": {},
        "citizenships": [],
        "dates": [],
        "events": [],
        "identifications": [],
        "image_urls": [],
        "industries" : [],
        "industry_codes" : [],
        "jurisdictions":[],
        "keywords":[],
        "languages": [],
        "loc":"00.00,00.00",
        "misc_info": {},
        "nationalities": [],
        "occupations": [],
        "physical_descriptions": [],
        "races": [],
        "remarks": [],
        "sex": "",
        "sources": [],
        "statuses": [],
        "scrape_name":"",
        "urls": []
    }
    */