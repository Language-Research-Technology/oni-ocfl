/* This is part of oni-ocfl

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
const path = require("path");
const fs = require("fs-extra");
const Provenance = require("./provenance.js");
const getLogger = require("./common/logger").getLogger;
const workingPath = require("./common").workingPath;
const { ROCrate, validate } = require("ro-crate");
const { program } = require('commander');
const ocfl = require("@ocfl/ocfl-fs");
//const shell = require("shelljs")
const generateArcpId = require("./mint-arcp-id");
const tmp = require("tmp");
const _ = require("lodash");
const assert = require("assert");
const { opendir } = require('fs').promises;
const ExcelJS = require('exceljs');
const modes = require("ro-crate-modes");
const { Preview, HtmlFile } = require("ro-crate-html");
const mainPackage = (function () {
  try {
    return require(path.join(require.main.path, "package.json"));
  } catch (e) { }
  return {};
})();


// OCFLObject
class CollectionObject {
  constructor(parent, crateDir) {
    this.collector = parent;
    const rocrateOpts = { alwaysAsArray: true, resolveLinks: true };
    if (crateDir) {
      // Load the RO-Crate from the specified directory
      console.log("CRATE DIR", crateDir)
      const metaPath = path.join(crateDir, "ro-crate-metadata.json");
      const json = JSON.parse(fs.readFileSync(metaPath));
      this.crate = new ROCrate(json, rocrateOpts);
    } else {
      this.crate = new ROCrate({}, rocrateOpts);
    }
    this.crate.resolveContext();
    this.rootDataset = this.crate.getRootDataset();
    this.rootDataset.hasPart = this.rootDataset.hasPart || [];
    this.rootDataset.hasMember = this.rootDataset.hasMember || [];
    fs.ensureDirSync(parent.tempDirPath);
    this.__tmpobj = tmp.dirSync({ tmpdir: parent.tempDirPath });
    this.dir = this.__tmpobj.name;
    // Make a temp directory
  }

  mintArcpId(paths, id) {
    if (Array.isArray(paths)) {
      if (id) {
        paths.push(id)
      }
    }
    if (typeof paths === 'string') {
      paths = [paths];
      if (id) {
        paths.push(id);
      }
    }
    this.id = generateArcpId(this.collector.namespace, paths);
    this.rootDataset["@id"] = this.id;
    const metadataDesc = this.crate.getItem(this.crate.defaults.roCrateMetadataID);
    metadataDesc.about = this.rootDataset;
  }

  // Copy a file into an objects temp directory and add it to the crate
  async addFile(f, srcDir, filePath, addToRootHasPart) {
    // File should be JSCON
    //f: IS a JSON-LD item
    // subDir is an optional directory under the colllectorDataDir
    // addToRootHasPart defaults to true,makes sure all files are linked stucturally as per RO-Crate spec
    // Set to false if file is already linked
    if (addToRootHasPart != false) {
      this.crate.pushValue(this.rootDataset, "hasPart", f);
    } else {
      this.crate.addItem(f);
    }
    var srcPath;

    if (filePath) {
      srcPath = path.join(srcDir, filePath)
    } else if (srcDir) {
      srcPath = path.join(srcDir, f["@id"])
    } else {
      srcPath = path.join(this.collector.dataDir, f["@id"]);
    }

    const destPath = path.join(this.dir, f["@id"]);

    if (fs.existsSync(srcPath)) {
      await fs.ensureFile(destPath);

      f.size = (await fs.stat(srcPath)).size;
      //console.log(srcPath, destPath)
      await fs.copyFile(srcPath, destPath);
      console.log("Copied", srcPath, destPath);
    } else {
      console.error(`WARNING MISSING FILE: ${srcPath}`);
    }
  }

  // Write data into a file and add it to the crate
  async writeFile(f, data) {
    // File should be JSCON
    //f: IS a JSON-LD item
    // subDir is an optional directory under the colllectorDataDir
    this.crate.pushValue(this.rootDataset, 'hasPart', f);
    const destPath = path.join(this.dir, f["@id"]);
    await fs.ensureFile(destPath);
    await fs.writeFile(destPath, data);
  }

  async generateHTML() {
    // Save an HTML file
    //console.log("Generating html from: " + metadataPath);
    //TODO: Add this as a library. Comenting out because it takes too long to search for this shell if not installed. No no
    //shell.exec(`rochtml "${metadataPath}"`);

    return (new HtmlFile(new Preview(this.crate))).render();
  }

  /** Adds item to repo
   * */
  async addToRepo(files = false, fileList = []) {
    // Write the object into the actual OCFL repo
    //fileList is an array of tuples ([[source,destination],[source,destination]]), files is boolean.
    //if files is true then files in the file list will be included in the object import. This allows files to be 
    //re-copied only when they've been updated rather than using addFile
    this.crate.addIdentifier({ name: this.collector.repoName, identifier: this.id });
    const localId = `_:local-id:${this.collector.repoName}:${this.id}`;
    const localRepoId = this.crate.getItem(localId);
    assert(localRepoId, 'Was not able to add identifier');
    this.crate.addEntity(this.collector.prov.scriptTool);
    this.crate.addEntity(this.collector.prov.createAction);
    const fileItems = {};
    try {
      const dir = await opendir(this.dir, { recursive: true });
      for await (const dirent of dir) {
        if (!dirent.isDirectory()) {
          let name = path.join(dirent.parentPath, dirent.name);
          name = path.relative(this.dir, name);
          fileItems[name] = {
            exists: true
          };
        }
      }
    } catch (e) {
      throw new Error(e);
    }

    const results = await validate(this.crate, fileItems);
    for (let r of results) {
      if (r.status === 'error') {
        let message = `Problem while adding to repository for ${this.id} error: ${r.id} : ${r.message}`;
        if (r.entity) {
          message += `: entity: ${r.entity}`;
        }
        throw new Error(message);
      }
    }
    try {
      //loop fileItems

      // validate crate
      let {excelValidator, modeValidator} = this.collector;
      if (excelValidator) {
        if (typeof excelValidator !== 'string') {
          excelValidator = 'ro-crate-validation.xlsx';
        }
        await validateWithExcel(excelValidator, this.crate);
      }
      if (modeValidator) {
        if (typeof modeValidator !== 'string') {
          modeValidator = 'https://language-research-technology.github.io/ro-crate-modes/modes/comprehensive-ldac.json';
        }
        await validateWithMode(modeValidator, this.crate);
      }

      const rocrateFile = path.join(this.dir, "ro-crate-metadata.json");
      const previewFile = path.join(this.dir, "ro-crate-preview.html");
      await fs.writeFile(rocrateFile, JSON.stringify(this.crate, null, 2));
      const previewContent = await this.generateHTML();
      await fs.writeFile(previewFile, previewContent);
      let object = this.collector.repo.object(this.id);
      let imports = [[this.dir, ""]]
      if (files && fileList.length > 0) {
        imports.push(...fileList);
      }
      await object.import(imports);
      console.log(`Wrote crate ${object}`);
    } catch (error) {
      console.error(error);
    }
    console.log(`Deleting crateDir: ${this.dir}`);
    fs.rmSync(this.dir, { recursive: true, force: true });
    console.log(`Deleted crateDir: ${this.dir}`)
  }
}

function getOpts(opts) {
  // extraOpts TODO: Array of arrays with extra .options (see below)
  program.option('-r, --repo-path <type>', 'Path to OCFL repository')
    .option('-n, --repo-name <type>', 'Name of OCFL repository')
    .option('-z, --repo-scratch <ns>', 'Path of the scratch ocfl repo')
    .option('-s, --namespace <ns>', 'namespace for ARCP IDs')
    .option('-c, --collection-name <ns>', 'Name of this collection (if not in template)')
    .option('-x --excel <file>', 'Excel file')
    .option('-vx --validate-with-excel [file]', 'Excel file for validation')
    .option('-vm --validate-with-mode [file]', 'A path or url to the mode file')
    .option('-p --temp-path <dirs>', 'Temporary Directory Path')
    .option('-t, --template <dirs>', 'RO-Crate directory on which to base this the RO-Crate metadata file will be used as a base and any files copied in to the new collection crate')
    .option('-d --data-dir <dirs>', "Directory of data files with sub directories '/Sound files' (for .wav) and '/Transcripts' (.csv)")
    .option('-D --debug <ns>', 'Use this in your collector to turn off some behaviour for debugging')
    .option('-m --multiple', 'Output multiple Objects rather than a single object')

  program.parse(process.argv);
  // merge the opts
  return { ...program.opts(), ...opts };
}

// Collector is a class for use in building (or adding to) an OCFL repo for a collection of data (eg a linguistic Collector)
class Collector {
  constructor(opts = {}) {
    this.opts = getOpts(opts);
    this.excelPath = this.opts.excel;
    this.tempDirPath = this.opts.tempPath || './temp';
    this.repoPath = this.opts.repoPath || "../repo";
    this.repoScratch = this.opts.repoScratch || "../scratch";
    this.repoName = this.opts.repoName || "repository";
    this.debug = this.opts.debug;
    if (this.debug == "true") { // Force type coercion
      this.debug = true;
      console.log('\n *** RUNNING IN DEBUG MODE *** \n');
    } else {
      this.debug = false;
    }
    this.templateCrateDir = this.opts.template;
    this.dataDir = this.opts.dataDir;
    this.excelFile = this.opts.excel;
    this.excelValidator = this.opts.validateWithExcel;
    this.namespace = this.opts.namespace; // eg "sydney-speaks" or "monash-Collector-of-english"
    this.CollectorName = this.opts.CollectorName;
    mainPackage.inputs = mainPackage.inputs || this.opts.inputs || { '@id': path.basename(this.opts.excel) };
    // This is slow so do it now
    this.prov = new Provenance(mainPackage);
    this.modeValidator = this.opts.validateWithMode;
  }

  async connect() {
    this.repo = ocfl.storage({
      root: this.repoPath, layout: {
        extensionName: '000N-path-direct-storage-layout'
      }
    });

    if (!await fs.pathExists(this.repoPath)) {
      console.log("CREATING")
      await this.repo.create();
      await this.repo.load();
    } else {
      await this.repo.load();
    }
  }

  newObject(cratePath) {
    return new CollectionObject(this, cratePath);

  }

}

const validateWorksheet = {
  /**
   * Handle properties worksheet
   * @param {ExcelJS.Worksheet} ws 
   * @param {ROCrate} roc 
   * @param {Array} errors 
   */
  types(ws, roc, errors) {
    const expected = {};
    let i, len = ws.rowCount;
    for (i = 2; i <= len; ++i) {
      const row = ws.getRow(i);
      if (row.hasValues) {
        const crate = row.getCell('crate').text || 'all';
        const type = row.getCell('type').text;
        if (type) {
          expected[crate] = expected[crate] || {};
          expected[crate][type] = row.getCell('count').value;
        }
      }
    }
    const realCount = {};
    for (const e of roc.entities()) {
      for (const t of e['@type']) {
        realCount[t] = realCount[t] || 0;
        realCount[t]++;
      }
    }
    //console.log(realCount);
    // match the count
    for (const ec of [expected.all, expected[roc.rootId]].filter(c => c)) {
      for (const t in ec) {
        if (ec[t] !== realCount[t]) {
          errors.push(`[validation] Entities ${t} expected count is ${ec[t]} but actual is ${realCount[t]}`);
        }
      }
    }
  },
  /**
   * Handle properties worksheet
   * @param {ExcelJS.Worksheet} ws 
   * @param {ROCrate} roc 
   * @param {Array} errors 
   */
  properties(ws, roc, errors) {
    const expected = {};
    let i, len = ws.rowCount;
    for (i = 2; i <= len; ++i) {
      const row = ws.getRow(i);
      if (row.hasValues) {
        const entity = row.getCell('entity').text;
        const property = row.getCell('property').text;
        if (entity && property) {
          expected[entity] = expected[entity] || {};
          expected[entity][property] = [row.getCell('count').value, row.getCell('value').text];
        }
      }
    }
    for (const entityId in expected) {
      const entity = entityId === './' ? roc.rootDataset : roc.getEntity(entityId);
      for (const prop in expected[entityId]) {
        const [expectedCount, expectedValue] = expected[entityId][prop];
        if (expectedCount != null) {
          if (entity[prop].length !== expectedCount) {
            errors.push(`[validation][${entityId}.${prop}] Expected value count of ${expectedCount} but got ${entity[prop].length}`);
          }
        } else if (expectedValue != null) {
          if (entity[prop][0] !== expectedValue) {
            errors.push(`[validation][${entityId}.${prop}] Expected value '${expectedValue}' but got '${entity[prop][0]}'`);
          }
        }
      }
    }
  }
};

/**
 * 
 * @param {string} excelFile 
 * @param {ROCrate} roc 
 */
async function validateWithExcel(excelFile, roc) {
  console.log('Validating crate using numbers in', excelFile);
  // Validate the RO-Crate using ExcelJS
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(excelFile);
  let errors = [];
  workbook.eachSheet((ws) => {
    const hRow = ws.getRow(1);
    // assign column name based on 1st row values to allow easy access
    hRow.eachCell((cell, colNumber) => {
      ws.getColumn(colNumber).key = cell.text.toLowerCase();
    });
    try {
      validateWorksheet[ws.name](ws, roc, errors);
    } catch (e) {
      console.error(e);
      errors.push(`[validation] Please check ${excelFile} file. Format error in worksheet ${ws.name}`);
    }
  });
  if (errors.length) {
    console.log(errors);
    throw new Error("Metadata stat does not pass validation");
  }
}

/**
 * 
 * @param {string} modeFile  A path or URL to mode file 
 * @param {ROCrate} roc 
 */
async function validateWithMode(modeFile, roc) {
  console.log('Validating crate using mode', modeFile);

  let errors = [];
  let mode;
  try {
    let res = await fetch(modeFile);
    if (res.ok) {
      mode = await res.json();
    }
  } catch (error) {
    mode = fs.readJsonSync(modeFile);
  }
  if (!mode) return;
  let results = modes.validate(mode, roc);
  for (let entityId in results) {
    for (let propId in results[entityId].props) {
      //errors.push(`[validation][mode][${entityId}] is missing required input ${input.name} of type ${input.type}`);
      let prop = results[entityId].props[propId];
      errors.push(...prop.errors.map(e => `[validation][mode][${entityId}][${prop.name}] ${e.description}`));
    }
  }
  if (errors.length) {
    console.log(errors);
    throw new Error("ro-crate-metadata does not pass mode validation");
  }
}
module.exports = Collector;
