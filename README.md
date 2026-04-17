# oni-ocfl

A Node.js library for ingesting datasets into an [OCFL](https://ocfl.io/) repository as [RO-Crate](https://www.researchobject.org/ro-crate/) objects. It is used in corpus-tools at [LDaCA](https://www.ldaca.edu.au/) to build and manage language data collections stored in OCFL.

## Installation

This library is used as a GitHub dependency (not published to npm):

```json
"oni-ocfl": "github:Language-Research-Technology/oni-ocfl#1.7.2"
```

In your `package.json`:

```json
{
  "dependencies": {
    "oni-ocfl": "github:Language-Research-Technology/oni-ocfl#1.7.2"
  }
}
```

Then run:

```bash
npm install
```

## Usage

```js
const { Collector, generateArcpId, Provenance } = require('oni-ocfl');
```

## Modules

### `Collector`

The main class for building an OCFL repository from a collection of RO-Crate objects. It manages the repository connection, creates `CollectionObject` instances, and drives the ingestion workflow.

```js
const collector = new Collector({
  repoPath: './repo',
  namespace: 'my-collection',
  dataDir: './data',
  templateCrateDir: './template'
});

await collector.connect(); // Creates or loads the OCFL repository
```

You can also use the static factory method:

```js
const collector = await Collector.create({ repoPath: './repo', namespace: 'my-collection' });
```

#### `collector.newObject(cratePath, crate?)`

Creates a new `CollectionObject`. Pass a directory path to load the RO-Crate metadata from disk, or pass an existing `ROCrate` instance directly:

```js
// Load from disk
const obj = collector.newObject('./path/to/crate-dir');

// Pass an ROCrate instance directly
const { ROCrate } = require('ro-crate');
const crate = new ROCrate(myJson, { alwaysAsArray: true, resolveLinks: true });
const obj = collector.newObject('./path/to/crate-dir', crate);
```

#### `CollectionObject`

Represents a single OCFL object being built. Key methods:

- **`mintArcpId(paths, id?)`** — Assigns an ARCP identifier to the root dataset (see `generateArcpId` below).
- **`importFile(source, target, entityProps?)`** — Queues a local file to be imported into the OCFL object.
- **`addFile(entity, srcDir, filePath?, addToRootHasPart?)`** — Copies a file into the temp directory and adds it to the crate.
- **`addToRepo(ignoreFilesInCrate?, files?)`** — Validates the crate and writes the object to the OCFL repository.

```js
const obj = collector.newObject(collector.templateCrateDir);
obj.mintArcpId(['corpus', 'my-dataset']);

obj.crate.rootDataset.name = 'My Dataset';
obj.crate.rootDataset.description = 'A language corpus';
obj.crate.rootDataset.datePublished = '2024';
obj.crate.rootDataset.license = 'cc-by-4.0';

await obj.addToRepo();
```

### `generateArcpId(namespace, paths)`

Generates an [ARCP](https://www.iana.org/assignments/uri-schemes/prov/arcp) URI used as the `@id` of an RO-Crate root dataset. ARCP URIs provide stable, namespace-scoped identifiers for objects in a collection.

```js
const { generateArcpId } = require('oni-ocfl');

const id = generateArcpId('my-collection', ['corpus', 'dataset-1']);
// => 'arcp://name,my-collection/corpus/dataset-1'
```

### `Provenance`

Records software provenance in the RO-Crate by adding a `SoftwareSourceCode` entity and a `CreateAction` entity. It reads metadata from the calling package's `package.json` (which must have `repository.url` and `description` fields).

```js
const { Provenance } = require('oni-ocfl');

const prov = new Provenance({
  name: 'my-corpus-tool',
  description: 'Tool for ingesting my corpus',
  repository: { url: 'https://github.com/my-org/my-corpus-tool' }
});
// prov.scriptTool  — SoftwareSourceCode entity added to the crate
// prov.createAction — CreateAction entity added to the crate
```

`Provenance` is instantiated automatically by `Collector` using the main process `package.json`. The entities are injected into every crate written by `addToRepo()`.

## CLI Options

`Collector` integrates with [Commander](https://github.com/tj/commander.js) and accepts command-line arguments that override constructor options:

| Flag | Description |
|------|-------------|
| `-r, --repo-path` | Path to OCFL repository |
| `-n, --repo-name` | Name of OCFL repository |
| `-s, --namespace` | Namespace for ARCP IDs |
| `-d, --data-dir` | Directory of data files |
| `-t, --template` | RO-Crate directory to use as base template |
| `-p, --temp-path` | Temporary directory path |
| `--vx, --validate-with-excel` | Excel file for crate validation |
| `--vm, --validate-with-mode` | Path or URL to an RO-Crate mode file for validation |
| `-D, --debug` | Enable debug mode |

## Tests

### Run all tests

```bash
npm test
```

### Run a single test file

Select a `*.spec.js` file in VS Code and use:

```
Current file run all tests
```

## Docs

[docs](./docs.md)

