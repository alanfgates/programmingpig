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

require 'pigudf'

class TestUDFs < PigUdf
    outputSchema "m:[chararray]"
    def constructMapFromBag(bag, name)
        print bag.class
        output = {}
        bag.each do |x|
            output[x[0]] = name
        end
        return output
    end

    outputSchema "keys:{(key:chararray)}"
    def getAllMapKeys(m)
        print m.class
        output = DataBag.new
        m.keys.each do |key|
            t = Array.new(1)
            t[0] = key
            output.add(t)
        end
        return output
    end
end
