#!/usr/bin/python
# This code is made available under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.

from org.apache.pig.scripting import *

P = Pig.compile("""
-- PR(A) = (1-d) + d (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))

previous_pagerank = load '$docs_in' as (url:chararray, pagerank:float,
                      links:{link:(url:chararray)});
outbound_pagerank = foreach previous_pagerank generate
                      pagerank / COUNT(links) as pagerank,
                      flatten(links) as to_url;
cogrpd            = cogroup outbound_pagerank by to_url,
                      previous_pagerank by url;
new_pagerank      = foreach cogrpd generate group as url,
                      (1 - $d) + $d * SUM (outbound_pagerank.pagerank)
                      as pagerank,
                      flatten(previous_pagerank.links) as links,
                      flatten(previous_pagerank.pagerank) AS previous_pagerank;
store new_pagerank into '$docs_out';
nonulls           = filter new_pagerank by previous_pagerank is not null and
                        pagerank is not null;
pagerank_diff     = foreach nonulls generate ABS (previous_pagerank - pagerank);
grpall            = group pagerank_diff all;
max_diff          = foreach grpall generate MAX (pagerank_diff);
store max_diff into '$max_diff';
""")

params = { 'd': '0.5', 'docs_in': 'webcrawl' }

for i in range(10):
    out = "out/pagerank_data_" + str(i + 1)
    max_diff = "out/max_diff_" + str(i + 1)
    params["docs_out"] = out
    params["max_diff"] = max_diff
    Pig.fs("rmr " + out)
    Pig.fs("rmr " + max_diff)
    bound = P.bind(params)
    stats = bound.runSingle()
    if not stats.isSuccessful():
        raise 'failed'
    mdv = float(str(stats.result("max_diff").iterator().next().get(0)))
    print "max_diff_value = " + str(mdv)
    if mdv < 0.01:
        print "done at iteration " + str(i)
        break
    params["docs_in"] = out
