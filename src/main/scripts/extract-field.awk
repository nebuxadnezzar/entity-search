#!/bin/gawk -f

BEGIN {
    BINMODE=2
    IGNORECASE=1
    FS=",";
    OFS="|";

    #for (zz = 1; zz < ARGC; zz++){ print "--->> " ARGV[zz] }
}

{

    /* printf("--> %s %g %g\n", $4, $20, $21); */

    # this part used to parse address from crimes data file
    #print $4, sprintf("%.2f",$20),sprintf("%.2f",$21)
    #print $4, $20, $21
    #print $4

    if( length($0) > 1)
    {
        print $0
    }


}



