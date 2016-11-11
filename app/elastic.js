'use strict';

var es = require('elasticsearch');
var fs = require('fs');

var client = new es.Client({
    host: 'localhost:9200',
    log: 'error',
    requestTimeout: 160000
});

module.exports.esClient = function () {
    return client;
};

module.exports.createIndex = function() {
    return client.indices.create({index:"cbp"});
};

module.exports.setMappings = function() {
    var mappings = JSON.parse(fs.readFileSync('../conf/mappings.json', 'utf8'));
    return client.indices.putMapping({index:"cbp", body:mappings,updateAllTypes:true, type:'wait-times'});
};