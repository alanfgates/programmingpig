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

# For streaming python UDF, uncomment the following line
#from pig_util import outputSchema

@outputSchema("m:[chararray]")
def constructMapFromBag(bag, value):
    output = {};
    for t in bag:
        output[t[0]] = value;
    return output;
    
@outputSchema("keys:{(key:chararray)}")
def getAllMapKeys(m):
    output = [];
    for key in m.iterkeys():
        output.append(key);
    return output;
