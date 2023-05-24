package com.entity.groovy;

public class entity_constants
{
    public static
    final ENTITY_SOURCE_TEMPLATE =
    '''
    {
        "name":"",
        "url":"",
        "description":""
    }
    ''';

    public static
    final ENTITY_ADDRESS_TEMPLATE =
    '''
    {
        "raw_format": "",
        "address1": "",
        "city": "",
        "province": "",
        "postal_code": "",
        "country": "",
        "birth_place": "false",
        "type":""
    }
    ''';

    public static
    final ENTITY_DATE_TEMPLATE =
    '''
    {
        "year": "",
        "month": "",
        "day": "",
        "circa": "false",
        "type":""
    }
    ''';

    public static
    final ENTITY_EVENT_TEMPLATE =
    '''
    {
        "description": "",
        "date": "",
        "category": "",
        "sub_category": ""
    }
    ''';

    public static
    final ENTITY_IDENTIFICATION_TEMPLATE =
    '''
    {
        "type": "",
        "country": "",
        "value": ""
    }
    ''';

    public static
    final ENTITY_TEMPLATE =
    '''
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
    ''';
}
