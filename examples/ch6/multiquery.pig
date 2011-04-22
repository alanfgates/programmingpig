-- This code is made available under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
-- WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
-- License for the specific language governing permissions and limitations
-- under the License.

players    = load 'baseball' as (name:chararray, team:chararray, 
				position:bag{t:(p:chararray)}, bat:map[]);
pwithba    = foreach players generate name, team, position,
				bat#'batting_average' as batavg;
byteam     = group pwithba by team;
avgbyteam  = foreach byteam generate group, AVG(pwithba.batavg);
store avgbyteam into 'by_team';
flattenpos = foreach pwithba generate name, team,
				flatten(position) as position, batavg;
bypos      = group flattenpos by position;
avgbypos   = foreach bypos generate group, AVG(flattenpos.batavg);
store avgbypos into 'by_position';
