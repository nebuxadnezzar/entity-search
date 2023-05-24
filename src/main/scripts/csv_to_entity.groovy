import java.sql.*;
import java.text.*;
//import java.util.concurrent.atomic.*;
import java.nio.file.*;
import groovy.json.*;
//import org.apache.lucene.analysis.util.CharArrayMap;
import com.entity.util.StringUtils.*;
import com.entity.util.*;
//import com.entity.tools.*;
import static com.entity.groovy.utils.*;
import com.entity.groovy.entity_factory;

DEFAULT_SEP = "\t";
TRUE_VALS = ['true','yes', 'y', '1' ] as Set;
BATCH_SIZE = 10000;

if( named_args.size() < 3 )
{
    println( "Usage: ${args[ 0 ]} conf=config_file.json in=input_file out=output_file" );

    println( "config file is JSON file. See example below" );
    println( "Example:\n" +
    '''
    {
        "separator":"\t",
        "trim":"true",
        "skiphdr":"true",
        "distincts":[0,1,2,3,5,8,29],
        "indexes":[0,1,2,3,5,8,29,101],
        "workdir": "c:\\temp\\ssm",
        "entity_mapping":
        {
            "name":0,
            "address":
            {
                "address1":1,
                "city":2,
                "postal_code":3,
                "country":5,
                "province":8,
                "type":29
            },
            "associations":
            { "parentOf":101 },
            "constants":{ "type":"O" }
        }
    }
    ''' );
    println( "where:\n" +
    '''
    separator - field separator,
    skiphdr - omit header, values true, yes, y are true everyting else is false
    indexes - list of indexes of all colums to be extracted from a CSV file
    distincts - list of indexes of distinct columns
    entity_mapping - map field offsets with specific properties
    ''' );
    println( "Example: ${args[ 0 ]} conf=bank_names_mapping.json in=bank_names.csv out=bank_entities.json" );
    return;
}

process( named_args['in'], named_args['out'], create_config( named_args['conf'] ) );

//-----------------------------------------------------------------------------
def process( in_file, out_file, props )
{
    println( props );
    def trim_flag = TRUE_VALS.contains( props['trim'] ),
        sep = props[ 'separator' ] ? props[ 'separator' ] : DEFAULT_SEP,
        skip_header = TRUE_VALS.contains( props[ 'skiphdr' ] ),
        work_dir = props[ 'workdir' ].trim(),
        timestamp =  new SimpleDateFormat("yyyyMMddHHmmss").format( new java.util.Date() ),
        expected_size = props.indexes.size();

    if( sep == '|' )
      { sep = '\\|'; }

    if( ! work_dir )
      { work_dir = "."; }

    SimpleUtils.makeDir( work_dir );

    work_dir += System.getProperty( "file.separator" );

    println( "${trim_flag} SEPARATOR [${sep}] ${skip_header} ${work_dir}" );

    open_temp_db( String.format( '%s%s.db', work_dir, timestamp ), props.indexes );

    def select_sql = create_distinct_select( props.indexes, props.distincts );
println( select_sql );
    def f = new File( in_file );

    if( ! f.exists() )
    {
        println "${in_file} does not exist";
        return;
    }

    def line_no = 0;

    try
    {
        f.eachLine
        { line, line_number ->

            line_no = line_number;

            if( line_number % 200 == 0 )
              { System.err.print( "\rline count: " + line_number ); }

            def l = line;

            if( l.size() > 0 && ( ! skip_header || line_number > 1 ) )
            {
                //println "\n==> " + l;

                def ss = sep != ',' ? l.split( sep, -1 ) : split_csv_record( l ),
                    sz = ss.length;

                //println( "SIZE ${sz}" ); ss.eachWithIndex{ s,i ->  println( "-> ${i} ${s}"); }

                if( sz < expected_size )
                {
                    System.err.printf( "\nInvalid record size on line %d - %d expected %d", line_number, sz, expected_size );
                    return;
                }

                def rec = extract_record( ss, props.indexes, trim_flag );
                //println( "\n==> " + rec );
                insert_rec( rec );
            }
        }
        _conn.commit();

        System.err.print( String.format( "\rline count: %d\n", line_no ) );

        def ef = new entity_factory(),
            parser = new JsonSlurper();

        remove_file( out_file );
        def of = new File( out_file );
        of.createNewFile();

        create_distinct_table( select_sql );
        create_entities( props.entity_mapping, ef, parser, of )
    }
    finally
    {
        cleanup();
    }

}
//-----------------------------------------------------------------------------
def create_entities( entity_mapping, entity_factory, json_parser, out_file )
{
    def sql = 'select coalesce( max(rowid), 0 ) as max_id from entity_data',
        rs  = _stmt.executeQuery( sql ),
        el  = [], // entity list
        maxid = rs.getInt( 'max_id' ),
        am = entity_mapping.address,       // address mapping
        asm = entity_mapping.associations, // assoc. mapping
        mism = entity_mapping.misc_info;

    println( "MAXID ${maxid}" );
    sql = 'select rowid as oid, e.* from entity_data e where rowid >= %d and rowid < %d';
    def cnt = 0;

    while( cnt < maxid )
    {
        println( String.format( sql, cnt, cnt + BATCH_SIZE ) );
        rs  = _stmt.executeQuery( String.format( sql, cnt, cnt + BATCH_SIZE ) );

        while( rs.next() )
        {
            def n = rs.getString( 't' + entity_mapping.name );

            if( StringUtils.isBlank( n ) )
            {
                System.err.println( "Entity name is missing" );
                continue;
            }

            def e = entity_factory.newEntity( rs.getString( 't' + entity_mapping.name ) ),
                a = entity_factory.newAddress();

            if( am != null )
            {
                am.each
                { k,v -> a[ k ] = rs.getString( String.format( 't%s', v ) ); }
                a.raw_format = "${a.address1} ${a.city} ${a.province} ${a.postal_code} ${a.country}";
            }

            if( asm != null )
            {
                asm.each
                { k,v ->
                    //println( "---> " + rs.getString( String.format( 't%s', v ) ) );
                    //println( "===> t${v} " + default_json( rs.getString( String.format( 't%s', v ) ) ) );
                    e.associations[ k ] = json_parser.parseText( default_json( rs.getString( String.format( 't%s', v ) ) ) );
                }
            }

            if( mism != null )
            {
                mism.each
                { k,v ->
                    //println( rs.getString( String.format( 't%s', v ) ) );
                    e.misc_info[ k ] = json_parser.parseText( default_json( rs.getString( String.format( 't%s', v ) ) ) );
                }
            }

            if( entity_mapping.constants != null )
            {
                entity_mapping.constants.each
                { k,v -> e[ k ] = v; }
            }

            if( StringUtils.isNotBlank( a.raw_format ) )
              { e.addresses.add( a ); }

            if( StringUtils.isBlank( e.data_source_id ) )
              { e.data_source_id = String.format( '%d', rs.getInt( 'oid' ) ); }

            if( el.size() >= BATCH_SIZE )
            {
                to_file( out_file, el );
                el = [];
            }

            el.add( e );
            //println( el );
        }
        rs.close();
        cnt += BATCH_SIZE;
    }

    to_file( out_file, el );

    return maxid;
}
//-----------------------------------------------------------------------------
def create_distinct_table( select_sql )
{
    def sql = "create table entity_data as " + select_sql;
    //println( sql );
    _stmt.executeUpdate( sql );
}
//-----------------------------------------------------------------------------
def insert_rec( rec )
{
    def sql = String.format( '''insert into file_data values( '%s'); ''', rec.join( "','" ) );
    //println( sql );
    _stmt.executeUpdate( sql );
}
//-----------------------------------------------------------------------------
def extract_record( ary, field_indexes, trim_flag )
{
    def rec = [];
    field_indexes.each
    {
        def s = normalize( ary[ it ] );
        rec << ( trim_flag && s != null ? s.trim() : s );
    }
    return rec;
}
//-----------------------------------------------------------------------------
def create_config( config_file )
{
    def parser = new JsonSlurper();
    return parser.parseText( new File( config_file ).text );
}
//-----------------------------------------------------------------------------
def cleanup()
{
    _stmt.close();
    _conn.commit();
    _conn.close();
}
//-----------------------------------------------------------------------------
def create_distinct_select( all_idx, dist_idx )
{
    def dl = all_idx - dist_idx, // difference list
        l = [],
        s1 = String.format( 't%s', all_idx.join( ',t' ) ),
        s2 = String.format( 't%s', dist_idx.join( ',t' ) ),
        ft = 't%s', // field template
        gt = ''''['||group_concat( nullif(t%s,''), ',' )||']' as t%s'''; // group template

    all_idx.each
    {
        def s = it.toString();
        l << ( it in dl ? String.format( gt,s,s) : String.format( ft, s ) );
    }

    def sql =
    '''
    with
    t as
    (
        select distinct %s
        from file_data
    )
    select %s
    from t
    group by %s
    ''';
    return String.format( sql, s1, l.join(',' ),s2 );
    /*

    with
    t as
    (
        select distinct t0,t1,t2,t3,t5,t8,t29,t101
        from file_data
    )
    select t0,t1,t2,t3,t5,t8,t29,group_concat( t101, char(9) )
    from t
    group by t0,t1,t2,t3,t5,t8,t29
    */
}
//-----------------------------------------------------------------------------
def create_tables( field_suffixes )
{
    def l1 = [],
        l2 = [],
        fs = 't%s text' // field string
        xs = 't%s'; // index string

    field_suffixes.each
    {
        def s = it.toString();
        l1 << String.format( fs, s );
        l2 << String.format( xs, s );
    }

    def sql = String.format( 'create table if not exists file_data(%s);', l1.join( ',' ) );
    //println( sql );
    _stmt.executeUpdate( sql );

    sql = String.format( 'create index if not exists fd_idx on file_data(%s);', l2.join( ',' ) );
    //println( sql );
    _stmt.executeUpdate( sql );
}
//-----------------------------------------------------------------------------
def open_temp_db( db_path, field_suffixes )
{

    Class.forName("org.sqlite.JDBC");
    _conn = DriverManager.getConnection("jdbc:sqlite:${db_path}");

    _conn.setTransactionIsolation( Connection.TRANSACTION_READ_UNCOMMITTED );
    System.out.println("Opened database successfully");

    _stmt = _conn.createStatement();

    // pragmas to speed up db inserts and lookups
    //
    _stmt.executeUpdate( "PRAGMA synchronous = OFF;" );
    _stmt.executeUpdate( "PRAGMA page_size = 65536;" );
    _stmt.executeUpdate( "PRAGMA cache_size = 16384;" );
    _stmt.executeUpdate( "PRAGMA temp_store = MEMORY;" );
    _stmt.executeUpdate( "PRAGMA journal_mode = OFF;" );

    _conn.setAutoCommit(false);

    def rs = _stmt.executeQuery( 'select distinct tbl_name from sqlite_master' ),
        ls = [] as Set;

    while ( rs.next() )
    {
        ls.add(  rs.getString( "tbl_name" ) );
    }
    rs.close();

    println( ls );

    create_tables( field_suffixes );
    _conn.commit();
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
def normalize( str )
{
    if( str == null )
      { return ''; }

    def t = str.replaceAll( '[.\\/;#@+)(]', ' ' )
               .replaceAll( "\\s+", ' ' )
               .replace( "'", "''" );

    if( t.replace( ' ', '' ).isNumber() )
    {
        t = t.replace( ' ', '' );
    }
    return t.trim();
}
//-----------------------------------------------------------------------------
def remove_file( old_path )
{
    def o = new File( old_path );

    if( o.exists() )
      { o.delete(); }
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
def default_json( s )
{
    if( s == null || s == "\0" )
      {  return 'null'; }
    return s;
}
//-----------------------------------------------------------------------------
def default_str( s )
{
    if( s == null || s == "\0" )
      {  return ''; }
    return s;
}
/*
awk -F"\t" "{print $1}" c:\temp\QV_Primary_small.txt
awk -F"\t" "{print $1$2}" c:\temp\QV_Primary_small.txt
awk -F"\t" "BEGIN{ OFS=\"---\"}{print $1,$2,sprintf( \"%s:%s\", $3,$4)}" c:\temp\QV_Primary_small.txt
awk -F"\t" "{printf(\"%s - %s\n\", $1,  $2)}" c:\temp\QV_Primary_small.txt
awk -F"\t" "BEGIN{ OFS=\"\t\"}{print $1,$2,$3,$4,$6,$9,$13,$30,sprintf( \"%s:%s\", $102,$103 )}" c:\temp\QV_Primary_small.txt > dump.txt
gawk 'BEGIN{a[1]=1; a[2]=2; print length(a); a[23]=45; print length(a)}'

awk -F "\t" "BEGIN{ OFS=\"\t\"}{gsub( /\"/,\"\", $1 ); gsub( /\"/,\"\", $2 ); gsub( /\"/,\"\", $6 ); gsub( /\"/,\"\", $13 ); gsub( /\"/,\"\", $102 ); gsub( /\"/,\"\", $103 ); print $1,$2,$3,$4,$6,$9,sprintf(\"\042%s\042\",$13),$30,sprintf(\"\042%s\042\",$102),sprintf( \"{\042name\042:\042%s\042,\042id\042:\042%s\042}\", $102,$103 )}" c:\temp\QV_Primary_small.txt > c:\temp\QV_Primary_preprocessed.txt

*/