const fs = require("fs");
const assert = require("assert");

class Provenance {
  constructor() {

    const nodePackage = require("../package");

    const softwareUrl = nodePackage.repository.url;
    assert(softwareUrl, 'Cannot build Provenance, please add repository.url to your package');
    const softwareName = nodePackage.name;
    const codeDescription = nodePackage.description;
    assert(codeDescription, 'Cannot build Provenance, please add description to your package');

    this.scriptTool = {
      "@id": softwareUrl,
      "@type": "SoftwareSourceCode",
      "name": softwareUrl,
      "description": codeDescription,
      "programmingLanguage": { "@id": "https://en.wikipedia.org/wiki/Node.js" }
    };

    this.createAction = {
      "@id": "#provenance",
      "name": `Created RO-Crate using ${softwareName}`,
      "@type": "CreateAction",
      "instrument": { "@id": softwareUrl },
      "result": { "@id": "ro-crate-metadata.json" }
    }
  }
}


module.exports = Provenance
