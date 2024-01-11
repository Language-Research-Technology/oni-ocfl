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
const {ROCrate} = require("ro-crate");
const {program} = require('commander');
const ocfl = require("@ocfl/ocfl-fs");
const shell = require("shelljs")
const generateArcpId = require("./mint-arcp-id");
const tmp = require("tmp");
const _ = require("lodash");
const assert = require("assert");


// OCFLObject
class CollectionObject {
  constructor(parent, crateDir) {
    this.collector = parent;
    const rocrateOpts = {alwaysAsArray: true, resolveLinks: true};
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
    this.__tmpobj = tmp.dirSync({tmpdir: parent.tempDirPath});
    this.dir = this.__tmpobj.name;
    // Make a temp directory
  }

  mintArcpId(paths, id) {
    if(Array.isArray(paths)) {
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

  async generateHTML(metadataPath) {
    // Save an HTML file
    //console.log("Generating html from: " + metadataPath);
    //TODO: Add this as a library. Comenting out because it takes too long to search for this shell if not installed. No no
    //shell.exec(`rochtml "${metadataPath}"`);
  }

  /** Adds item to repo
   * */
  async addToRepo() {
    // Write the object into the actual OCFL repo
    this.crate.addIdentifier({name: this.collector.repoName, identifier: this.id});
    const localId = `_:local-id:${this.collector.repoName}:${this.id}`;
    const localRepoId = this.crate.getItem(localId);
    assert(localRepoId, 'Was not able to add identifier');
    this.crate.addEntity(this.collector.prov.scriptTool);
    this.crate.addEntity(this.collector.prov.createAction);
    const rocrateFile = path.join(this.dir, "ro-crate-metadata.json");
    await fs.writeFileSync(rocrateFile, JSON.stringify(this.crate, null, 2));
    await this.generateHTML(rocrateFile);
    let object = this.collector.repo.object(this.id);
    
    await object.import(this.dir);
    console.log(`Wrote crate ${object}`);
    console.log(`Deleting crateDir: ${this.dir}`);
    fs.rmSync(this.dir, {recursive: true, force: true});
    console.log(
      "Deleted"
    )
  }
}

function getOpts(extaOpts) {
  // extraOpts TODO: Array of arrays with extra .options (see below)
  program.option('-r, --repo-path <type>', 'Path to OCFL repository')
    .option('-n, --repo-name <type>', 'Name of OCFL repository')
    .option('-z, --repo-scratch <ns>', 'Path of the scratch ocfl repo')
    .option('-s, --namespace <ns>', 'namespace for ARCP IDs')
    .option('-c, --collection-name <ns>', 'Name of this collection (if not in template)')
    .option('-x --excel <file>', 'Excel file')
    .option('-p --temp-path <dirs>', 'Temporary Directory Path')
    .option('-t, --template <dirs>', 'RO-Crate directory on which to base this the RO-Crate metadata file will be used as a base and any files copied in to the new collection crate')
    .option('-d --data-dir <dirs>', "Directory of data files with sub directories '/Sound files' (for .wav) and '/Transcripts' (.csv)")
    .option('-D --debug <ns>', 'Use this in your collector to turn off some behaviour for debugging')
    .option('-m --multiple', 'Output multiple Objects rather than a single object')

  program.parse(process.argv);
  return program.opts();
}

// Collector is a class for use in building (or adding to) an OCFL repo for a collection of data (eg a linguistic Collector)
class Collector {
  constructor(opts) {
    // For testing, pass opts
    if (opts) {
      // We have 'normal' options being passed
      this.opts = opts;
    } else {
      // Get from commandline - TODO - pass more opts
      this.opts = getOpts(opts);
    }

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
    this.namespace = this.opts.namespace; // eg "sydney-speaks" or "monash-Collector-of-english"
    this.CollectorName = this.opts.CollectorName;
    // This is slow so do it now
    this.prov = new Provenance();
  }

  async connect() {
    this.repo = ocfl.storage({root: this.repoPath, layout: {
        extensionName: '000N-path-direct-storage-layout'
      }});

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


module.exports = Collector;
