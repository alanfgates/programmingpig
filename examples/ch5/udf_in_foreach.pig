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

divs  = load 'NYSE_dividends' as (exchange, symbol, date, dividends);
--make sure all strings are upper case
upped = foreach divs generate UPPER(symbol) as symbol, dividends;
grpd  = group upped by symbol;   --output a bag upped for each value of symbol
--take a bag of integers, produce one result for each group
sums  = foreach grpd generate group, SUM(upped.dividends);
dump sums;
