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
});

app.get('/index', function (req, res) {
    res.send(ingest.ingest());
});

app.get('/mapping', function(req,rsp) {
    es.createIndex()
        .then((response) => {
            console.log(response)
            es.setMappings().then(
                (response) => {
                    console.log(response);
                    rsp.send(response);
                },
                (err) => {
                    console.log(err);
                    rsp.send(err);
                }
            )
        }, (err) =>{
            console.log(err);
            rsp.send(err);
        });
});