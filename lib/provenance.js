const fs = require("fs");
const assert = require("assert");

class Provenance {
  // inputs example: [{ "@id": "COOEE_contents.xlsx" }]
  constructor({ repository, name, description, inputs } = {}) {
    const codeRepository = repository?.url;
    assert(repository, 'Cannot build Provenance, please add repository.url to your package');
    assert(description, 'Cannot build Provenance, please add description to your package');

    this.scriptTool = {
      "@id": codeRepository,
      "@type": "SoftwareSourceCode",
      name,
      description,
      codeRepository,
      programmingLanguage: "ECMAScript",
      runtimePlarform: "Node.js"
    };

    this.createAction = {
      "@id": "#provenance",
      "@type": "CreateAction",
      name: `Create RO-Crate using ${name}`,
      instrument: { "@id": codeRepository },
      result: { "@id": "ro-crate-metadata.json" }
    };

    if (inputs) {
      this.createAction.object = inputs;
    }
  }
}


module.exports = Provenance
