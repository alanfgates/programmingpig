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

players = load 'baseball' as (name:chararray, team:chararray,
			position:bag{t:(p:chararray)}, bat:map[]);
noempty = foreach players generate name,
			((position is null or IsEmpty(position)) ? {('unknown')} : position)
			as position; 
pos     = foreach noempty generate name, flatten(position) as position;
bypos   = group pos by position;
dump bypos;
