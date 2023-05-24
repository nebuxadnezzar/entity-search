package com.entity.groovy;

import static com.entity.scraping.util.StringUtils.*;
import com.entity.scraping.util.Metaphone;

public class entity_utils
{
    private static final entityTypes = [ 'A', 'O', 'P' ] as Set;
    private static final Metaphone m3 = new Metaphone( true, true);
    
    public static def getMetaphone( var )
    {
        return m3.encode( var );
    }
    //-------------------------------------------------------------------------
    public static def setNameSoundexes( entity_list )
    {
        Metaphone m3 = new Metaphone( true, true);

        for( e in entity_list )
        {
            e.soundex = ( m3.encode( e.stripped_name ) );
        }
        return entity_list;
    }
    //-------------------------------------------------------------------------
    public static def getStrippedName( val )
    {
        return stripPunctuation( val ).replaceAll( "\\s+", "" ).toUpperCase();
    }
    //-------------------------------------------------------------------------
    public static def isEntityValid( e )
    {
        return ( e && isNotBlank( e.name ) 
                   && isNotBlank( e.type ) 
                   && isNotBlank( e.stripped_name )
                   && isNotBlank( e.soundex )
                   && entityTypes.contains( e.type ) );
    }
}