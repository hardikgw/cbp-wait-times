'use strict';

let fs = require('fs');

let FileWriter = function (filename) {
    this.filename = filename;
    this.headers = [];
};
FileWriter.prototype.write = function (data) {
    if (!this.headerWritten) {
        this.headers = Object.keys(data).sort(function(a, b) {
            let nameA = a.toUpperCase();
            let nameB = b.toUpperCase();
            if (nameA < nameB) {
                return -1;
            }
            if (nameA > nameB) {
                return 1;
            }

            // names must be equal
            return 0;
        });
        fs.writeFileSync(this.filename, this.headers.concat() + "\n");
        this.headerWritten = true;
    }
    let values = [];
    for (const header of this.headers) {
        values.push(data[header]);
    }
    try {
        fs.appendFileSync(this.filename, values.concat() + "\n");
    } catch (e) {
        console.write(e);
    }
};
module.exports.FileWriter = FileWriter;
