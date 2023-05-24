package com.entity.util;

/**
  this is a wrapper class for Metaphone3
  because of the licencing restrictions we cannot change scope of Metaphone3 methods.
  we need them to be public, not package
*/

public class Metaphone
{
    private Metaphone3 m3 = null;
    
    public Metaphone( boolean encodeVowels, boolean encodeExact )
    {
        m3 = new Metaphone3();
        m3.SetEncodeVowels( encodeVowels );
        m3.SetEncodeExact( encodeExact );
    }
    //-------------------------------------------------------------------------
    public String encode( String str )
    {
        m3.SetWord( str );
        m3.Encode();
        return( String.format( "%s_%s", m3.GetMetaph(), m3.GetAlternateMetaph() ) );
    }
}