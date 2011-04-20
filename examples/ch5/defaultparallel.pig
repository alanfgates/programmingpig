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

set default_parallel 10;
daily   = load 'NYSE_daily' as (exchange, symbol, date, open, high, low, close,
			volume, adj_close);
bysymbl = group daily by symbol;
average = foreach bysymbl generate group, AVG(daily.close) as avg;
sorted  = order average by avg desc;
dump sorted;

