'use strict'

var es = require('elasticsearch');
var fs = require('fs');

var client = new es.Client({
    host: 'localhost:9200',
    log: 'trace'
});

exports.esClient = function () {
    return client;
};

exports.setMappings = function() {
    var mappings = JSON.parse(fs.readFileSync('/Users/hp/workbench/projects/cbp/wait-times/www/conf/mappings.json', 'utf8'));
    client.indices.create({index:"cbp"});
    client.indices.putMapping({index:"cbp", body:mappings,updateAllTypes:true, type:'wait-times'}, function(val){
        console.log(val)
    });
};