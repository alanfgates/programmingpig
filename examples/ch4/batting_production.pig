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

register 'production.py' using jython as bballudfs;
players  = load 'baseball' as (name:chararray, team:chararray,
				pos:bag{t:(p:chararray)}, bat:map[]);
nonnull  = filter players by bat#'slugging_percentage' is not null and
				bat#'on_base_percentage' is not null;
calcprod = foreach nonnull generate name, bballudfs.production(
				(float)bat#'slugging_percentage',
				(float)bat#'on_base_percentage');
dump calcprod;
