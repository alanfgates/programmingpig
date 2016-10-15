DEFINE INTERSECT_EXCEPT_BASE2(a, b, mode) returns outputs {
  DEFINE replicateRows com.acme.intersectexcept.ReplicateRows('$mode');
  a1 = group $a by $0..;
  a2 = foreach a1 generate group, COUNT(a);
  b1 = group $b by $0..;
  b2 = foreach b1 generate group, COUNT(b);
  c = cogroup a2 by group, b2 by group;
  d = foreach c generate replicateRows(a2, b2), flatten(group);
  $outputs = foreach d generate flatten($0), $1..;
};

DEFINE INTERSECT2(a, b) returns outputs {
  $outputs = INTERSECT_EXCEPT_BASE2($a, $b, 'INTERSECT');
};

DEFINE INTERSECT_ALL2(a, b) returns outputs {
  $outputs = INTERSECT_EXCEPT_BASE2($a, $b, 'INTERSECT_ALL');
};

DEFINE EXCEPT2(a, b) returns outputs {
  $outputs = INTERSECT_EXCEPT_BASE2($a, $b, 'EXCEPT');
};

DEFINE EXCEPT_ALL2(a, b) returns outputs {
  $outputs = INTERSECT_EXCEPT_BASE2($a, $b, 'EXCEPT_ALL');
};

DEFINE INTERSECT_EXCEPT_BASE3(a, b, c, mode) returns outputs {
  DEFINE replicateRows com.acme.intersectexcept.ReplicateRows('$mode');
  a1 = group $a by $0..;
  a2 = foreach a1 generate group, COUNT(a);
  b1 = group $b by $0..;
  b2 = foreach b1 generate group, COUNT(b);
  c1 = group $c by $0..;
  c2 = foreach c1 generate group, COUNT(c);
  d = cogroup a2 by group, b2 by group, c2 by group;
  e = foreach d generate replicateRows(a2, b2, c2), flatten(group);
  $outputs = foreach e generate flatten($0), $1..;
};

DEFINE INTERSECT3(a, b, c) returns outputs {
  $outputs = INTERSECT_EXCEPT_BASE3($a, $b, $c, 'INTERSECT');
};

DEFINE INTERSECT_DISTINCT3(a, b, c) returns outputs {
  $outputs = INTERSECT_EXCEPT_BASE3($a, $b, $c, 'INTERSECT_DISTINCT');
};

DEFINE EXCEPT3(a, b, c) returns outputs {
  $outputs = INTERSECT_EXCEPT_BASE3($a, $b, $c, 'EXCEPT');
};

DEFINE EXCEPT_ALL3(a, b, c) returns outputs {
  $outputs = INTERSECT_EXCEPT_BASE3($a, $b, $c, 'EXCEPT_ALL');
};
