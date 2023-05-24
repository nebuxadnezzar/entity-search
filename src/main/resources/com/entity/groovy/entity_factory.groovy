package com.entity.groovy;

import groovy.json.*;
import static entity_constants.*;
import static static_data.*;
import com.entity.util.Metaphone;

public class entity_factory
{
    def jsonSlurper = new JsonSlurper();
    def dateTypes = ['DOB', 'DOD', 'INCORPORATED', 'DISSOLVED' ] as Set;
    def assocTypes = [ 'PARENT', 'CHILD', 'PARTNER', 'STUDENT', 'PARTICIPANT', 'EMPLOYEE' ];

    //-------------------------------------------------------------------------
    public def newEntity( name )
    {
        if( ! name )
          { throw new RuntimeException( "Entity name cannot be empty" ); }

        def m3 = new Metaphone( true, true);
        def o = jsonSlurper.parseText( ENTITY_TEMPLATE );
        o.name = name;
        o.jurisdictions = [] as Set;
        o.industries = [] as Set;
        o.industry_codes = [] as Set;
        o.stripped_name = name.replaceAll( "\\p{Punct}", "" )
                              .replaceAll( "\\s+", "" )
                              .toUpperCase();
        o.soundex = m3.encode( o.stripped_name );
        return o;
    }
    //-------------------------------------------------------------------------
    public def newEvent( category )
    {
        if( ! category )
          { return null; }

        def o = jsonSlurper.parseText( ENTITY_EVENT_TEMPLATE );
        o.category = category;
        return o;
    }
    //-------------------------------------------------------------------------
    public def newAddress()
    {
       return jsonSlurper.parseText( ENTITY_ADDRESS_TEMPLATE );
    }
    //-------------------------------------------------------------------------
    public def newSource()
    {
       return jsonSlurper.parseText( ENTITY_SOURCE_TEMPLATE );
    }
    //-------------------------------------------------------------------------
    public def newIdentification()
    {
       return jsonSlurper.parseText( ENTITY_IDENTIFICATION_TEMPLATE );
    }
    //-------------------------------------------------------------------------
    public def newAssociation( entity, ass_type )
    {
        if( ! assocTypes.contains( ass_type ) )
          { throw RuntimeException( "Invalid association type ${ass_type}\nValid types are ${assocTypes}" ); }

        def t = null;

        switch( ass_type )
        {
            case 'PARENT': t = 'parentOf'; break;
            case 'CHILD': t = 'childOf'; break;
            case 'PARTNER': t = 'partnerOf'; break;
            case 'STUDENT': t = 'studentOf'; break;
            case 'PARTICIPANT': t = 'participantOf'; break;
            case 'EMPLOYEE': t = 'employeeOf'; break;
            default:
                 t = 'participantOf';
        }
        entity.associations[ t ] = [] as Set;
    }
    //-------------------------------------------------------------------------
    public def newDate( date_str )
    {
        return newDate( date_str, 'DOB' );
    }
    //-------------------------------------------------------------------------
    //-- @param date MM/dd/yyyy - note circa years are -/-/yyyy format.
    //-------------------------------------------------------------------------
    public def newDate( date_str, date_type )
    {
       if( ! date_str )
          { return null; }

        if( ! dateTypes.contains( date_type ) )
          { throw RuntimeException( "Invalid date type ${date_type}\nValid types are ${dateTypes}" ); }

        def o = jsonSlurper.parseText( ENTITY_DATE_TEMPLATE ),
            dp = date_str.split("/");

        o.type = date_type;

        if( dp && dp.length == 3 )
        {
            o.year = dp[ 2 ];

            if( dp[ 0 ] == '-' )
              { o.circa = 'true'; }
            else
            {
                o.day = dp[ 1 ];
                o.month = dp[ 0 ];
            }
        }

        if( ( o.year && o.year.length() < 4 ) || ! o.year.isInteger() )
          { throw new IllegalArgumentException("Invalid year[" + date_str + "]" ); }

        if( o.circa.equalsIgnoreCase( 'false' ) )
        {
            if( ! month_codes.find( { it == o.month} ) )
              { throw new IllegalArgumentException("Invalid month[" + date_str + "]" ); }

            if( ( o.day && o.day.length() > 2 ) || ! o.day.isInteger() || Integer.valueOf( o.day ) > 31 )
              { throw new IllegalArgumentException("Invalid day[" + date_str + "]" ); }
        }

        return o;
    }
    //-------------------------------------------------------------------------
    public static void main( args )
    {
/*
        def ef = new entity_factory();
        def e = ef.newEntity( 'joe shmoe' );
        def ev = ef.newEvent( 'BRB' );
        def a = ef.newAddress();
        def s = ef.newSource();
        def d = ef.newDate( '12/11/1967' );
        def d1 = ef.newDate( '-/-/1960' );
        def i = ef.newIdentification();
        def ass = ef.newAssociation( 'PARENT' );
        i.type = 'PASSPORT';
        i.country = 'USA';
        s.url = 'microsoft.com';
        a.country = 'USA';
        e.events.add( ev );
        e.addresses.add( a );
        e.sources.add( s );
        e.dates.add( d );
        e.dates.add( d1 );
        e.identifications.add( i );
        ass.parentOf = 'ORACLE';
        e.associations.add( ass );
        ass = ef.newAssociation( 'STUDENT' );
        ass.studentOf = 'NYU';
        e.associations.add( ass );
        //println( ev );
        //println( e );
        println( JsonOutput.prettyPrint( JsonOutput.toJson( e ) ).stripIndent() );
        */
    }
}