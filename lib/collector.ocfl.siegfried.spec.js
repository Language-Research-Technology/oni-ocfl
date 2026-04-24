const assert = require("assert");
const { Collector, generateArcpId } = require('../index');
Collector.mainPackage = require('../package.json');
const { ROCrate } = require("ro-crate");
const path = require('path');
const rimraf = require('rimraf');
const fs = require('fs-extra');

const basePath = 'test-data/collector_ocfl';
const namespace = 'collector-test';
const templateCrateDir = path.join(basePath, 'template');
const datePublished = '2025';
const license = 'cc-by-4.0';

describe("Siegfried cache data", function () {
  this.timeout(10000);

  const sfRepoPath = path.join(basePath, 'ocfl-sf');
  const sfDataDir = path.join(basePath, 'files-sf');
  const sfCachePath = path.join(sfDataDir, '.siegfried-test.json');

  // Minimal Siegfried JSON output with two file entries
  const sfCacheData = {
    siegfried: "1.11.0",
    files: [
      {
        filename: path.join(sfDataDir, "sample.csv"),
        matches: [{ mime: "text/csv", id: "x-fmt/18", basis: ["extension match csv"] }]
      },
      {
        filename: path.join(sfDataDir, "image.png"),
        matches: [{ mime: "image/png", id: "fmt/11", basis: ["byte match at 0"] }]
      }
    ]
  };

  let sfCollector;

  before(async function () {
    rimraf.sync(sfRepoPath);
    await fs.ensureDir(sfDataDir);
    await fs.writeJson(sfCachePath, sfCacheData);
    sfCollector = new Collector({ repoPath: sfRepoPath, namespace, dataDir: sfDataDir, templateCrateDir });
    await sfCollector.connect();
  });

  after(async function () {
    rimraf.sync(sfRepoPath);
    await fs.remove(sfDataDir);
  });

  it('loads siegfried cache into sfData map', async function () {
    await sfCollector.runSiegfried(false, sfCachePath);
    assert.equal(sfCollector.sfData.size, 2, 'sfData should have two entries');
    assert.ok(sfCollector.sfData.has('sample.csv'), 'sfData should contain sample.csv');
    assert.ok(sfCollector.sfData.has('image.png'), 'sfData should contain image.png');
  });

  it('sfData entries contain the original siegfried file metadata', async function () {
    await sfCollector.runSiegfried(false, sfCachePath);
    const csvEntry = sfCollector.sfData.get('sample.csv');
    assert.equal(csvEntry.matches[0].mime, 'text/csv');
    assert.equal(csvEntry.matches[0].id, 'x-fmt/18');

    const pngEntry = sfCollector.sfData.get('image.png');
    assert.equal(pngEntry.matches[0].mime, 'image/png');
    assert.equal(pngEntry.matches[0].id, 'fmt/11');
  });

  it('clears previous sfData entries when cache is reloaded', async function () {
    await sfCollector.runSiegfried(false, sfCachePath);
    assert.equal(sfCollector.sfData.size, 2);

    // Write a reduced cache (only one file) and reload
    const reducedCache = {
      siegfried: "1.11.0",
      files: [sfCacheData.files[0]]
    };
    const reducedCachePath = path.join(sfDataDir, '.siegfried-reduced.json');
    await fs.writeJson(reducedCachePath, reducedCache);

    await sfCollector.runSiegfried(false, reducedCachePath);
    assert.equal(sfCollector.sfData.size, 1, 'sfData should be cleared and repopulated');
    assert.ok(sfCollector.sfData.has('sample.csv'));
    assert.ok(!sfCollector.sfData.has('image.png'), 'image.png should no longer be in sfData');
  });

  it('falls back to running sf when cache file does not exist and clears stale sfData', async function () {
    // Pre-populate sfData with a known entry that should be cleared on reload
    sfCollector.sfData.set('stale-entry.txt', { matches: [] });
    assert.ok(sfCollector.sfData.has('stale-entry.txt'), 'stale entry should exist before reload');

    const missingPath = path.join(sfDataDir, '.does-not-exist.json');
    await sfCollector.runSiegfried(false, missingPath);

    // Regardless of whether sf ran or failed, sfData must have been cleared
    assert.ok(!sfCollector.sfData.has('stale-entry.txt'), 'stale-entry.txt should be cleared from sfData');
  });

  it('applies encodingFormat from sfData to file entities when adding to repo', async function () {
    // Write a real file that can be stat-ed so _processFiles succeeds
    const csvFilePath = path.join(sfDataDir, 'sample.csv');
    await fs.writeFile(csvFilePath, 'col1,col2\n1,2\n');

    await sfCollector.runSiegfried(false, sfCachePath);

    const crate = new ROCrate({}, { alwaysAsArray: true, resolveLinks: true });
    // Pass sfDataDir as crateDir so _processFiles resolves 'sample.csv' relative to sfDataDir
    const obj = sfCollector.newObject(sfDataDir, crate);
    obj.mintArcpId(['sf', 'encoding-test']);
    const crateId = generateArcpId(sfCollector.namespace, ['sf', 'encoding-test']);
    crate.rootId = crateId;
    crate.rootDataset.name = 'SF Encoding Test';
    crate.rootDataset.description = 'Tests encodingFormat enrichment from siegfried data';
    crate.rootDataset.datePublished = datePublished;
    crate.rootDataset.license = license;
    crate.addEntity({ '@id': 'sample.csv', '@type': 'File', name: 'Sample CSV' });

    await obj.addToRepo();

    // Read back the saved crate and inspect the file entity
    const savedObject = sfCollector.repo.object(crateId);
    await savedObject.load();
    const crateJson = JSON.parse(await savedObject.getFile({ logicalPath: 'ro-crate-metadata.json' }).asString());
    const savedCrate = new ROCrate(crateJson, { alwaysAsArray: true, resolveLinks: true });
    const fileEntity = savedCrate.getEntity('sample.csv');
    assert.ok(fileEntity, 'File entity should exist in saved crate');
    const encodingFormats = [].concat(fileEntity.encodingFormat || []);
    const mimeValues = encodingFormats.map(f => (typeof f === 'object' ? f['@value'] ?? f['@id'] : f));
    assert.ok(
      mimeValues.some(v => v === 'text/csv') || encodingFormats.some(f => f === 'text/csv' || f?.['@value'] === 'text/csv'),
      'encodingFormat should include text/csv from siegfried data'
    );
  });
});
