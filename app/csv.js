'use strict';

let fs = require('fs');

let FileWriter = function (filename) {
    this.filename = filename;
};
FileWriter.prototype.write = function (data) {
    this.headers = [];
    if (!this.headerWritten) {
        this.headers = Object.keys(data);
        fs.writeFileSync(this.filename, this.headers.concat() + "\n");
        this.headerWritten = true;
    }
    let values = [];
    for (const header of this.headers) {
        values.push(data[header]);
    }
    fs.appendFileSync(this.filename, values.concat() + "\n");
};
module.exports.FileWriter = FileWriter;
