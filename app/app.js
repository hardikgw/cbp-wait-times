'use strict'

var express = require('express');
var es = require('./elastic');
var ingest = require('./ingest');

var app = express();

app.get('/', function (req, res) {
    res.send('CBP Wait Times');
});

app.listen(3000, function () {
    console.log('Example app listening on port 3000!');
    es.setMappings();
    ingest.getData();
});