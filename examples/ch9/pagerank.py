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

previous_pagerank = LOAD '$docs_in' USING PigStorage('\t')
                      AS (url:chararray, pagerank:float,
                          links:{link:(url:chararray)});
outbound_pagerank = FOREACH previous_pagerank GENERATE
                      pagerank / COUNT(links) AS pagerank,
                      FLATTEN(links) AS to_url;
cogrpd            = COGROUP outbound_pagerank BY to_url,
                      previous_pagerank BY url;
new_pagerank      = FOREACH cogrpd GENERATE group AS url,
                      (1 - $d) + $d * SUM (outbound_pagerank.pagerank)
                      AS pagerank,
                      FLATTEN(previous_pagerank.links) AS links;
STORE new_pagerank INTO '$docs_out' USING PigStorage('\t');
""")

params = { 'd': '0.5', 'docs_in': 'data/webcrawl' }

for i in range(10):
    out = "out/pagerank_data_" + str(i + 1)
    params["docs_out"] = out
    Pig.fs("rmr " + out)
    bound = P.bind(params)
    stats = bound.runSingle()
    if not stats.isSuccessful():
        raise 'failed'
    params["docs_in"] = out



