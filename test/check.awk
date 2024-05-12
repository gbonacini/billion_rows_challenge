#!/usr/bin/awk -f

BEGIN {
        RS = "\n"
        FS = ";"
        MIN=0
        MAX=0
        SUM=0
        COUNT=0
}
{
        if( $2 < MIN ) MIN = $2
        if( $2 > MAX ) MAX = $2
        COUNT = COUNT + 1
        SUM   = SUM + $2


}
END {
        printf "MIN: %f\nAVG: %f\nMAX: %f\n", MIN, SUM / COUNT , MAX
}

