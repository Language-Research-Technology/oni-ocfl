/* This is part of RO-Crate-language tools 

(c) The University of Queensland 2021

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

/* Test for collection.js */

const assert = require("assert");
const Provenance = require("./provenance.js")
var expect = require('chai').expect;

describe("Add provenance", function () {
  it("Should make a prov object", async function () {
    const provenance = new Provenance();
    assert.equal(provenance.scriptTool.name, "git+https://github.com/Language-Research-Technology/oni-ocfl.git")
  });
});
