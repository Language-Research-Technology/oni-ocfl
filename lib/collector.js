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
const os = require("os");
const Provenance = require("./provenance.js");
const getLogger = require("./common/logger").getLogger;
const workingPath = require("./common").workingPath;
const { ROCrate, validate } = require("ro-crate");
const { createCommand } = require('commander');
const ocfl = require("@ocfl/ocfl-fs");
//const shell = require("shelljs")
const generateArcpId = require("./mint-arcp-id");
const tmp = require("tmp");
const _ = require("lodash");
const assert = require("assert");
//const { opendir } = require('fs').promises;
const ExcelJS = require('exceljs');
const modes = require("ro-crate-modes");
const { Preview, HtmlFile } = require("ro-crate-html");

const mainPackage = (function () {
  try {
    return require(path.join(require.main.path, "package.json"));
  } catch (e) { }
  return {};
})();

function isRelFilePath(url) {
  try {
    new URL(url);
    return false;
  } catch (e) {
    if (url.startsWith('#')) return false;
    return true;
  }
}
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
    this.rootDataset = this.crate.root;
    fs.ensureDirSync(parent.tempDirPath);
    this.__tmpobj = tmp.dirSync({ tmpdir: parent.tempDirPath });
    this.dir = this.__tmpobj.name;
    this.files = {};
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
    // const metadataDesc = this.crate.getItem(this.crate.defaults.roCrateMetadataID);
    // metadataDesc.about = this.rootDataset;
  }

  /**
   * Add a file or directory in the local file system to the queue to be imported to the OCFL repository.
   * @param {string} source 
   * @param {string} target 
   * @param {object} entityProps If specified, a file entity will be created and added to the crate.
   */
  importFile(source, target, entityProps) {
    this.files[target] = {
      exists: true,
      source,
      target
    };
    if (entityProps) {
      const fileEntity = {
        '@id': target,
        '@type': 'File',
        name: target,
        ...entityProps
      };
      if (fileEntity.isPartOf) {
        for (const p of [].concat(fileEntity.isPartOf)) {
          this.crate.addValues(p, 'hasPart', fileEntity);
        }
      } else {
        this.crate.addEntity(fileEntity);
      }
    }
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

  /**
   * Write data into a file in the temp dir and add it to the crate
   * @param {*} f 
   * @param {*} data 
   */
  async writeFile(f, data) {
    // File should be JSCON
    //f: IS a JSON-LD item
    // subDir is an optional directory under the colllectorDataDir
    this.crate.addValues(this.root, 'hasPart', f);
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

  /**
   * Iterate through all the entities in the crate with type File and ensure that the file is imported.
   */
  async _processFiles() {
    const { dataDir, templateCrateDir } = this.collector;
    for (let entity of this.crate.entities()) {
      const entityId = entity['@id'];
      if (entity['@type'].includes('File') && isRelFilePath(entityId)) {
        let file = this.files[entityId];
        if (!file) {
          const source = path.join(dataDir ?? templateCrateDir, entityId);
          this.importFile(source, entityId);
          file = this.files[entityId];
        }
        try {
          const stats = await fs.stat(file.source);
          file.exists = true;
          entity.contentSize = '' + stats.size;
        } catch (error) {
          file.exists = false;
        }
      }
    }
  }

  /**
   * Create an OCFL object in the repository and add all the required files.
   * By default, all the file defined in the File entities in the crate will be imported to the OCFL automatically.
   * To avoid that set ignoreFilesInCrate to false, for example, during testing. 
   * Use the files parameter to manually re-import only some specific files.
   * @param {boolean} [ignoreFilesInCrate] if true then existing File entities in the crate will not be automatically added to the OCFL repo,
   *   and ROCrate validator will not be validating against the files.
   * @param {[string, string][]} [files] If specified, each file in the array of tuple [source,destination][] will be imported to the OCFL object,
   *   in addition the to the already existing files added using importFile() method. This is the same as calling importFile() on each of the tuple prior. 
   */
  async addToRepo(ignoreFilesInCrate = false, files) {
    await this.crate.resolveContext();

    this.crate.addIdentifier({ name: this.collector.repoName, identifier: this.id });
    const localId = `_:local-id:${this.collector.repoName}:${this.id}`;
    const localRepoId = this.crate.hasEntity(localId);
    assert(localRepoId, 'Was not able to add identifier');
    this.crate.addEntity(this.collector.prov.scriptTool);
    this.crate.addEntity(this.collector.prov.createAction);
    if (!ignoreFilesInCrate) {
      await this._processFiles();
    }

    const results = await validate(this.crate, ignoreFilesInCrate ? undefined : this.files);
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
      // validate crate
      let { excelValidator, modeValidator } = this.collector;
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

      // const rocrateFile = path.join(this.dir, "ro-crate-metadata.json");
      // const previewFile = path.join(this.dir, "ro-crate-preview.html");
      //await fs.writeFile(rocrateFile, JSON.stringify(this.crate, null, 2));
      const previewContent = await this.generateHTML();
      //await fs.writeFile(previewFile, previewContent);
      let object = this.collector.repo.object(this.id);
      // let imports = [[this.dir, ""]]
      // if (files && fileList.length > 0) {
      //   imports.push(...fileList);
      // }
      // await object.import(imports);
      await object.update(async (t) => {
        await t.write('ro-crate-metadata.json', JSON.stringify(this.crate, null, 2));
        await t.write('ro-crate-preview.html', previewContent);
        await t.import(this.dir, "");
        for (const target in this.files) {
          const { source } = this.files[target];
          await t.import(source, target);
        }
        if (Array.isArray(files)) {
          for (const [source, target] of files) {
            await t.import(source, target);
          }
        }
      })
      console.log(`Wrote crate ${object}`);
    } catch (error) {
      console.error(error);
    }
    console.log(`Deleting crateDir: ${this.dir}`);
    fs.rmSync(this.dir, { recursive: true, force: true });
    console.log(`Deleted crateDir: ${this.dir}`)
  }
}

const defaultTempDir = fs.realpathSync(os.tmpdir());

function getOpts(opts) {
  // extraOpts TODO: Array of arrays with extra .options (see below)
  const program = createCommand();
  program.option('-r, --repo-path <type>', 'Path to OCFL repository')
    .option('-n, --repo-name <type>', 'Name of OCFL repository')
    .option('-z, --repo-scratch <ns>', 'Path of the scratch ocfl repo')
    .option('-s, --namespace <ns>', 'namespace for ARCP IDs')
    .option('-c, --collection-name <ns>', 'Name of this collection (if not in template)')
    .option('-x --excel <file>', 'Excel file')
    .option('--vx, --validate-with-excel [file]', 'Excel file for validation')
    .option('--vm, --validate-with-mode [file]', 'A path or url to the mode file')
    .option('-p --temp-path <dirs>', 'Temporary Directory Path')
    .option('-t, --template <dirs>', 'RO-Crate directory on which to base this the RO-Crate metadata file will be used as a base and any files copied in to the new collection crate')
    .option('-d --data-dir <dirs>', "Directory of data files with sub directories '/Sound files' (for .wav) and '/Transcripts' (.csv)")
    .option('-D --debug <ns>', 'Use this in your collector to turn off some behaviour for debugging')
    .option('-m --multiple', 'Output multiple Objects rather than a single object')
  program.allowExcessArguments(true);
  program.parse(process.argv);
  // merge the opts
  return { ...program.opts(), ...opts };
}

// Collector is a class for use in building (or adding to) an OCFL repo for a collection of data (eg a linguistic Collector)
class Collector {
  static mainPackage = mainPackage;

  constructor(opts = {}) {
    this.opts = getOpts(opts);
    this.excelPath = this.opts.excel;
    this.tempDirPath = this.opts.tempPath || defaultTempDir;
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
    let pkg = Collector.mainPackage;
    pkg.inputs = pkg.inputs || this.opts.inputs || this.opts.excel ? { '@id': path.basename(this.opts.excel) } : undefined;
    // This is slow so do it now
    this.prov = new Provenance(pkg);
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
