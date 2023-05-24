package com.entity.groovy;

import java.util.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.StandardOpenOption;
import groovy.io.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.*;
import static groovy.io.FileType.*;

import com.entity.indexing.Indexer;

public class utils
{
    public static final LOAD_PROPS = 1; // key=value file
    public static final LOAD_XML   = 2; // <entry key="x"> value </entry> file
    public static final LOAD_UNK   = 3; // free text file - 2b implemented
    //-------------------------------------------------------------------------
    public static getPropertiesMap( path )
    {
       return getPropertiesMap( path, LOAD_PROPS );
    }
    //-------------------------------------------------------------------------
    public static getPropertiesMap( path, props_type )
    {
        def props = new Properties();

        try
        {
            def _in = new FileInputStream( path );

            switch( props_type )
            {
                case LOAD_XML:
                    props.loadFromXML( _in );
                    break;
                case LOAD_PROPS:
                    props.load( _in );
                    break;
                default:
                    props.load( _in );
            }
        }
        catch( Exception e )
        {
            System.err.println( e );
            e.printStackTrace();
        }
        return props;
    }

    public static getRandomAccessFile( path )
    {
        return new RandomAccessFile( path );
    }
    //-------------------------------------------------------------------------
    public static process_schema( schema_file )
    {
        def xml = new XmlSlurper().parse( schema_file );
        def schema = [:]/*, tbl_name = xml.attributes()[ 'name' ]*/;

        //println( 'XML ATTR ' + xml.attributes() );

        xml.field.each
        { f ->
            def m = f.attributes(), s;
            schema[ m.name ] = [ : ];

            //println( m );

            Indexer.FIELD_OPTS.each
            {  k ->

                s = ( m[ k ] ? m[ k ].trim() : null );

                switch( k )
                {
                    case 'index' :
                    case 'store' :
                    case 'raw'   :
                    case 'sort'  :
                         schema[ m.name ][ k ] = Indexer.validateTrueType( s ) ? 'true' : 'false';
                         break;
                    case 'type' :
                         schema[ m.name ][ k ] = Indexer.validateDataType( s ) ? s.toLowerCase() : 'string';
                         break;
                }
            }
        }

        return schema;

    }
    //-------------------------------------------------------------------------
    public static get_file_list( path )
    {
        def d = new File( new File( path ).getParentFile().getAbsolutePath() );
        def prefix = d.getAbsolutePath()+ System.getProperty( "file.separator" );
        def p = path.replace( prefix, '' )
                    .replace( ".", "[.]").replace( "*", ".*" );
        def names = []
        def pp = java.util.regex.Pattern.compile( p,
                                               java.util.regex.Pattern.DOTALL |
                                               java.util.regex.Pattern.CASE_INSENSITIVE )
        if( d.isDirectory() )
        {
            d.eachFileMatch FILES, pp, { names << prefix + it.name }
        }

        return names;
    }
    //-------------------------------------------------------------------------

    //----------------------------------------------------------------------------
    public static drain_results( results )
    {
       results.each
       { result ->
          try
          {
             result.get( 8000, TimeUnit.SECONDS );
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
    public static shutdown( pool, results )
    {
       drain_results( results );
       pool.shutdown();
       pool.awaitTermination( 600, TimeUnit.SECONDS);
    }
}


class RandomAccessFile
{
    java.io.RandomAccessFile mFile = null;
    //FileChannel      mFc   = null;
    //ByteBuffer       mMb   = null;

    public RandomAccessFile( path )
    {
        //println( "PATH " + path );
        mFile = new java.io.RandomAccessFile( path, "rw");
        mFile.seek( 0 );
    }

    public position()
    {
        return mFile.getFilePointer();
    }

    public writeStr( data )
    {
        mFile.write( data.getBytes() );
    }

    public writeStr( data, pos )
    {
        mFile.seek( pos );
        mFile.write( data.getBytes() );
    }

    public readLine()
    {
        return mFile.readLine();
    }

    public RandomAccessFile getFile()
    {
        return mFile;
    }

    public void close()
    {
        try
        {
            mFile.close();
            //mFc.close();
        }
        catch( Exception e )
        {
            System.out.println( e );
            e.printStackTrace();
        }
        System.gc();
    }
}