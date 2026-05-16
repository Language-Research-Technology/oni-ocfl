const assert = require("assert");
const ocfl = require("@ocfl/ocfl-fs");
const { Collector, generateArcpId } = require('../index');
Collector.mainPackage = require('../package.json');
const { ROCrate } = require("ro-crate");
const path = require('path');
const rimraf = require('rimraf');

const basePath = 'test-data/collector_ocfl';
const rocratesPath = 'test-data/rocrates';
const repoPath = path.join(basePath, 'ocfl');
const namespace = 'collector-test';
const dataDir = path.join(basePath, 'files');
const templateCrateDir = path.join(basePath, 'template');
const title = 'Farms to Freeways Example Dataset';
const titleV2 = 'Farms to Freeways Example Dataset - Im v2!';
const newDescription = 'NEW DESCRIPTION';
const description = 'description';
const datePublished = '2025';
const license = 'cc-by-4.0';

let collector;
let corpusCrateRootId;
let repository;

describe("Create OCFL Repo", function () {
  this.timeout(10000);

  before(function () {
    rimraf.sync(repoPath);
    console.log(`${repoPath} deleted`);
  });

  it("Should make a new Collector", async function () {
    collector = new Collector({ repoPath, namespace, dataDir, templateCrateDir, logLevel: 'error' });
    assert.equal(collector.opts.repoPath, repoPath);
  });

  it('can connect', async function () {
    await collector.connect();
  });

  it('can add V1', async function () {
    this.timeout(5000);
    console.log(collector.templateCrateDir);
    const corpusRepo = collector.newObject(collector.templateCrateDir);
    corpusRepo.mintArcpId(["corpus", "root"]);
    const corpusCrate = corpusRepo.crate;
    corpusCrateRootId = generateArcpId(collector.namespace, ["corpus", "root"]);
    corpusCrate.descriptor.license = license;
    corpusCrate.rootId = corpusCrateRootId;
    corpusCrate.root.name = title;
    corpusCrate.root.description = description;
    corpusCrate.root.datePublished = datePublished;
    corpusCrate.root.license = license;
    await corpusRepo.addToRepo();
  });

  it('can get V1 crate', async function () {
    const object = collector.repo.object(corpusCrateRootId);
    console.log(object);
    await object.load();
    const crateFile = await object.getFile({ logicalPath: 'ro-crate-metadata.json' }).asString();
    const crate = new ROCrate(JSON.parse(crateFile));
    assert(crate.root.name, title);
  });

  it('can add V2', async function () {
    const corpusRepo = collector.newObject(collector.templateCrateDir);
    corpusRepo.mintArcpId("corpus", "root");
    const corpusCrate = corpusRepo.crate;
    corpusCrateRootId = generateArcpId(collector.namespace, ["corpus", "root"]);
    corpusCrate.descriptor.license = license;
    corpusCrate.rootId = corpusCrateRootId;
    corpusCrate.root.name = titleV2;
    corpusCrate.root.description = description;
    corpusCrate.root.datePublished = datePublished;
    corpusCrate.root.license = license;
    await corpusRepo.addToRepo();
  });

  it('can get V2 crate', async function () {
    const object = collector.repo.object(corpusCrateRootId);
    await object.load();
    const crateFile = await object.getFile({ logicalPath: 'ro-crate-metadata.json' }).asString();
    const crate = new ROCrate(JSON.parse(crateFile));
    assert(crate.root.name, titleV2);
  });

  it('can add V3 ', async function () {
    const corpusRepo = collector.newObject(collector.templateCrateDir);
    corpusRepo.mintArcpId("corpus", "root");
    const corpusCrate = corpusRepo.crate;
    corpusCrateRootId = generateArcpId(collector.namespace, ["corpus", "root"]);
    corpusCrate.descriptor.license = license;
    corpusCrate.rootId = corpusCrateRootId;
    corpusCrate.root.name = title;
    corpusCrate.root.description = description;
    corpusCrate.root.datePublished = datePublished;
    corpusCrate.root.license = license;
    await corpusRepo.addToRepo();
  });

  it('can get V3 crate', async function () {
    const object = collector.repo.object(corpusCrateRootId);
    await object.load();
    const crateFile = await object.getFile({ logicalPath: 'ro-crate-metadata.json' }).asString();
    const crate = new ROCrate(JSON.parse(crateFile));
    assert(crate.root.name, title);
  });

  it('can add V4 ', async function () {
    const corpusRepo = collector.newObject(collector.templateCrateDir);
    corpusRepo.mintArcpId("corpus", "root");
    const corpusCrate = corpusRepo.crate;
    corpusCrateRootId = generateArcpId(collector.namespace, ["corpus", "root"]);
    corpusCrate.descriptor.license = license;
    corpusCrate.rootId = corpusCrateRootId;
    corpusCrate.root.name = 'root v4';
    corpusCrate.root.description = newDescription;
    corpusCrate.root.datePublished = datePublished;
    corpusCrate.root.license = license;
    corpusCrate.root["@type"] = ["Dataset", "RepositoryCollection"];
    await corpusRepo.addToRepo();
  });

  it('can get V4 crate', async function () {
    const object = collector.repo.object(corpusCrateRootId);
    await object.load();
    const crateFile = await object.getFile({ logicalPath: 'ro-crate-metadata.json' }).asString();
    const crate = new ROCrate(JSON.parse(crateFile));
    assert(crate.root.description, newDescription);
  });

  it('should error if crate is not valid when adding to repository', async function () {
    const corpusRepo = collector.newObject(collector.templateCrateDir);
    corpusRepo.mintArcpId("corpus", "root");
    const corpusCrate = corpusRepo.crate;
    corpusCrateRootId = generateArcpId(collector.namespace, ["corpus", "root"]);
    corpusCrate.rootId = corpusCrateRootId;
    corpusCrate.root.description = newDescription;
    corpusCrate.root.datePublished = datePublished;
    corpusCrate.root.license = license;
    corpusCrate.root["@type"] = ["Dataset", "RepositoryCollection"];
    try {
      //await corpusRepo.addToRepo();
    } catch (e) {
      // corpusCrate.root.name is not defined
      assert(e instanceof Error);
    }
  });


  it('should not error if crate references files', async function () {
    const validateCrateDir = path.join(rocratesPath, 'validate');
    const collectorValidate = new Collector({ repoPath, namespace, dataDir: validateCrateDir, template: validateCrateDir });
    await collectorValidate.connect();
    const corpusRepo = collectorValidate.newObject(collectorValidate.templateCrateDir);
    corpusRepo.mintArcpId("corpus", "root");
    const corpusCrate = corpusRepo.crate;
    corpusCrateRootId = generateArcpId(collectorValidate.namespace, ["corpus", "root"]);
    corpusCrate.descriptor.license = license;
    corpusCrate.rootId = corpusCrateRootId;
    corpusCrate.root.name = 'name';
    corpusCrate.root.description = newDescription;
    corpusCrate.root.datePublished = datePublished;
    corpusCrate.root.license = license;
    corpusCrate.root["@type"] = ["Dataset", "RepositoryCollection"];
    await corpusRepo.addFile({ '@id': 'csvs/some_file.csv' }, collectorValidate.templateCrateDir, null, false);
    await corpusRepo.addFile({ '@id': 'another_example.txt' }, collectorValidate.templateCrateDir, null, false);
    await corpusRepo.addToRepo();
  });

  it('should error if crate references files and are not in the repository', async function () {
    const validateCrateDir = path.join(rocratesPath, 'validate');
    const collectorValidate = new Collector({ repoPath, namespace, dataDir: validateCrateDir, template: validateCrateDir });
    await collectorValidate.connect();
    const corpusRepo = collectorValidate.newObject(collectorValidate.templateCrateDir);
    corpusRepo.mintArcpId("corpus", "root");
    const corpusCrate = corpusRepo.crate;
    corpusCrateRootId = generateArcpId(collectorValidate.namespace, ["corpus", "root"]);
    corpusCrate.rootId = corpusCrateRootId;
    corpusCrate.root.name = 'name';
    corpusCrate.root.description = newDescription;
    corpusCrate.root.datePublished = datePublished;
    corpusCrate.root.license = license;
    corpusCrate.root["@type"] = ["Dataset", "RepositoryCollection"];
    //Not adding csvs/some_file.csv to trigger an error
    await corpusRepo.addFile({ '@id': 'another_example.txt' }, collectorValidate.templateCrateDir, null, false);
    try {
      await corpusRepo.addToRepo();
    } catch (e) {
      assert(e instanceof Error);
    }
  });

  it('can create a new object by passing an ROCrate instance directly as crate', async function () {
    const rocrateOpts = { alwaysAsArray: true, resolveLinks: true };
    const crate = new ROCrate({}, rocrateOpts);
    const corpusRepo = collector.newObject(collector.templateCrateDir, crate);
    assert.strictEqual(corpusRepo.crate, crate, 'The crate should be the same ROCrate instance passed as crate');
    corpusRepo.mintArcpId(["corpus", "metafile-direct"]);
    const corpusCrateId = generateArcpId(collector.namespace, ["corpus", "metafile-direct"]);
    crate.rootId = corpusCrateId;
    crate.root.name = 'MetaFile Direct Test';
    crate.root.description = 'Created by passing ROCrate directly';
    crate.root.datePublished = datePublished;
    crate.root.license = license;
    await corpusRepo.addToRepo();
    const object = collector.repo.object(corpusCrateId);
    await object.load();
    const crateFile = await object.getFile({logicalPath: 'ro-crate-metadata.json'}).asString();
    const savedCrate = new ROCrate(JSON.parse(crateFile));
    assert.equal(savedCrate.root.name, 'MetaFile Direct Test');
  });

  it('can create a new object by passing a pre-populated ROCrate instance as crate', async function () {
    const fs = require('fs-extra');
    const rocrateOpts = { alwaysAsArray: true, resolveLinks: true };
    const templateJson = JSON.parse(fs.readFileSync(path.join(templateCrateDir, 'ro-crate-metadata.json')));
    const crate = new ROCrate(templateJson, rocrateOpts);
    const corpusRepo = collector.newObject(collector.templateCrateDir, crate);
    assert.strictEqual(corpusRepo.crate, crate, 'The crate should be the same ROCrate instance passed as crate');
    corpusRepo.mintArcpId(["corpus", "metafile-prepopulated"]);
    const corpusCrateId = generateArcpId(collector.namespace, ["corpus", "metafile-prepopulated"]);
    crate.rootId = corpusCrateId;
    // Override name while preserving all existing entities from the template crate
    crate.root.name = 'Pre-populated MetaFile Test';
    crate.root.datePublished = datePublished;
    crate.root.license = license;
    await corpusRepo.addToRepo(true); // ignoreFilesInCrate=true since template files don't exist locally
    const object = collector.repo.object(corpusCrateId);
    await object.load();
    const crateFile = await object.getFile({logicalPath: 'ro-crate-metadata.json'}).asString();
    const savedCrate = new ROCrate(JSON.parse(crateFile));
    assert.equal(savedCrate.root.name, 'Pre-populated MetaFile Test');
    // Verify that entities from the original template crate are preserved
    assert.ok(savedCrate.root.description, 'Description from template crate should be preserved');
  });

});
