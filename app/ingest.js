var fs = require('fs');
var parse = require('csv-parse');
var es = require('./elastic');
var _ = require('lodash');
var basePath = '/Users/hp/workbench/projects/cbp/wait-times/data';

var getAirports = function() {
    return new Promise(function(resolve, reject){
        var airports = {};
        var rs = fs.createReadStream(basePath + '/airports/airports.csv');
        var airportparser = parse({columns: true});
        airportparser.on('readable', function () {
            var USAirports = ['US', 'AS', 'GU', 'MP', 'PR', 'CA'];
            while (airport = airportparser.read()) {
                if (USAirports.indexOf(airport.country_code)>-1) {
                    var cleanairport = _.omitBy(airport, _.isEmpty);
                    airports[cleanairport.airport_code] = {
                        lat: cleanairport.lat,
                        lon: cleanairport.lon,
                        city: cleanairport.city_name
                    };
                }
            }
        });
        airportparser.on('error', function(err) {
            console.log(err.message);
            reject(err);
        });
        airportparser.on('finish', function(){
            resolve(airports);
        });
        rs.pipe(airportparser);
    });
};

var getData = function(file, airports, callback) {
    var errors = [];
    var created = 0;
    var errored = 0;
    var parser = parse({columns: true}, function (err, data) {
        for (var i=0;i<data.length;i++) {
            var waittime = _.omitBy(data[i], _.isEmpty);
            waittime["location"] = {
                lat: airports[waittime.Airport].lat,
                lon: airports[waittime.Airport].lon
            };
            waittime["lat"] = airports[waittime.Airport].lat;
            waittime["lon"] = airports[waittime.Airport].lon;
            es.esClient().create({
                index: 'cbp',
                type: 'wait-times',
                timeout: -1,
                id: (waittime.Airport + waittime.Terminal + '_' + waittime.Date.replace(/\//g, '_') + '_' + waittime.Hour.replace(/ /g,'')).replace(/ /g,'_'),
                body: waittime
            }).then((res) => {
                created++
            }, (err) => {
                errors.push({ error : error});
                errored ++;
            });
        }
    });
    parser.on('error', function(err) {
        console.log(err.message);
        reject(err);
    });
    parser.on('finish', function(){
        console.log({created:created,errrored:errored,errors:errors});
    });

    var datafiles = [];
    var files = fs.readdirSync(basePath);
    var rs = fs.createReadStream(basePath + "/" + file);
    rs.pipe(parser);
};

module.exports.ingest = function(){
    getAirports().then((val) => {
        var files = fs.readdirSync(basePath);
        _.remove( files, function ( file ) {
            return !file.endsWith('.csv')
        });
        // var x = 0;
        // var loopArray = function(arr) {
        //     getData(arr[x],val,function(){
        //         x++;
        //         if(x < arr.length) {
        //             loopArray(arr);
        //         }
        //     });
        // };
        // loopArray(files);
        files.forEach((file) => {
            getData(file, val);
        });
    });
};

