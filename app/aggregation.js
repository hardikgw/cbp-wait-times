var fs = require('fs');
var parse = require('csv-parse');
var es = require('./elastic');
var _ = require('lodash');
var basePath = '/Users/hp/workbench/projects/cbp/wait-times/data';



var rowsByAirportPerDay = function() {
    var filename = basePath + '/WaitTimesPerDay.csv';
    fs.writeFileSync(filename, "Airport,Date,AvgWait,MaxWait,Booths,Lat,Lon" + "\n");
    es.esClient().search({
        index: 'cbp',
        size: 0,
        body: {
            query : {
                match_all : {}
            },
            aggs: {
                airport: {
                    terms: {
                        field: "Airport",
                        size : 1000
                    },
                    aggs: {
                        date: {
                            terms: {
                                field: "Date",
                                size : 5000
                            },
                            aggs : {
                                _Average : {
                                    avg : {
                                        field : "Average"
                                    }
                                },
                                _Max : {
                                    avg : {
                                        field : "Max"
                                    }
                                },
                                _Booths : {
                                    avg : {
                                        field : "Booths"
                                    }
                                },
                                _Lat : {
                                    avg : {
                                        field : "lat"
                                    }
                                },
                                _Lon : {
                                    avg : {
                                        field : "lon"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }).then((results)=>{
        var airports = results.aggregations.airport.buckets;
        airports.forEach((airport) => {
            airport.date.buckets.forEach((date)=>{
                var fields = [];
                fields.push(airport.key);
                fields.push(date.key_as_string);
                fields.push(date._Average.value);
                fields.push(date._Max.value);
                fields.push(date._Booths.value);
                fields.push(date._Lat.value);
                fields.push(date._Lon.value);
                fs.appendFileSync(filename, fields.concat() + "\n");
            });
        });
        return "done";
    },(err)=> {
        console.log(err);
        return (err)
    });
};



module.exports.generateCsv = function () {
    return rowsByAirportPerDay();
};