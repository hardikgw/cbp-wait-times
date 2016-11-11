'use strict';

var es = require('elasticsearch');
var fs = require('fs');

var client = new es.Client({
    host: 'localhost:9200',
    log: 'error',
    requestTimeout: 160000
});

exports.esClient = function () {
    return client;
};
exports.createIndex = new Promise(function(resolve, reject) {
    client.indices.create({index:"cbp"}, function(err, response, status){
        if (err) {
            reject(response);
        } else {
            resolve(response);
        }
    });
});

exports.setMappings = new Promise(function(resolve, reject) {
    var mappings = JSON.parse(fs.readFileSync('/Users/hp/workbench/projects/cbp/wait-times/www/conf/mappings.json', 'utf8'));
    client.indices.putMapping({index:"cbp", body:mappings,updateAllTypes:true, type:'wait-times'}, function(err, response, status){
        if (err) {
            reject(response);
        } else {
            resolve(response);
        }
    });
});