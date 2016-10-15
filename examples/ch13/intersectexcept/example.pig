register intersectexcept.jar
import 'intersect_except.pig';

a = load 'A' as (color:chararray);
b = load 'B' as (colorname:chararray);
c = load 'C' as (colorname:chararray);
d = INTERSECT3(a, b, c);
dump d;
