'use strict';

let fs = require('fs');
let parse = require('csv-parse');
let es = require('./elastic');
let _ = require('lodash');
let basePath = '/Users/hp/workbench/projects/cbp/wait-times/data';
const FileWriter = require("./csv");

let rowsByAirportPerDay = function () {
    let wfilename = basePath + '/WaitTimesPerDay.csv';
    let fw = new FileWriter.FileWriter(wfilename);
    es.esClient().search({
        index: 'cbp',
        size: 0,
        body: {
            query: {
                match_all: {}
            },
            aggs: {
                airport: {
                    terms: {
                        field: "Airport",
                        size: 1000
                    },
                    aggs: {
                        date: {
                            terms: {
                                field: "Date",
                                size: 5000
                            },
                            aggs: {
                                _Average: {
                                    avg: {
                                        field: "Average"
                                    }
                                },
                                _Max: {
                                    avg: {
                                        field: "Max"
                                    }
                                },
                                _Booths: {
                                    avg: {
                                        field: "Booths"
                                    }
                                },
                                _Total: {
                                    sum: {
                                        field: "Total"
                                    }
                                },
                                _Lat: {
                                    avg: {
                                        field: "lat"
                                    }
                                },
                                _Lon: {
                                    avg: {
                                        field: "lon"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }).then((results)=> {
        var airports = results.aggregations.airport.buckets;
        airports.forEach((airport) => {
            airport.date.buckets.forEach((date)=> {
                let fields = {};
                fields["Airport"] = airport.key;
                fields["LongDate"] = date.key;
                fields["Date"] = date.key_as_string;
                fields["AvgWait"] = date._Average.value;
                fields["MaxWait"] = date._Max.value;
                fields["Booths"] = date._Booths.value;
                fields["Count"] = date._Total.value;
                fields["Lat"] = date._Lat.value;
                fields["Lon"] = date._Lon.value;
                fw.write(fields);
            });
        });
        return "done";
    }, (err)=> {
        console.log(err);
        return (err)
    });
};


let rowsByAirportPerHour = function () {
    let wfilename = basePath + '/WaitTimesPerHour.csv';
    let fw = new FileWriter.FileWriter(wfilename);
    es.esClient().search({
        index: 'cbp',
        size: 0,
        body: {
            query: {
                match_all: {}
            },
            aggs: {
                airport: {
                    terms: {
                        field: "Airport",
                        size: 1000
                    },
                    aggs: {
                        date: {
                            terms: {
                                field: "Date",
                                size: 5000
                            },
                            aggs: {
                                hour: {
                                    terms: {
                                        field: "Hour",
                                        size: 25
                                    },
                                    aggs: {
                                        _Average: {
                                            avg: {
                                                field: "Average"
                                            }
                                        },
                                        _Max: {
                                            avg: {
                                                field: "Max"
                                            }
                                        },
                                        _Booths: {
                                            avg: {
                                                field: "Booths"
                                            }
                                        },
                                        _Total: {
                                            sum: {
                                                field: "Total"
                                            }
                                        },
                                        _Lat: {
                                            avg: {
                                                field: "lat"
                                            }
                                        },
                                        _Lon: {
                                            avg: {
                                                field: "lon"
                                            }
                                        }
                                    }
                                }
                            }

                        }
                    }
                }
            }
        }
    }).then((results)=> {
        results.aggregations.airport.buckets.forEach((airport) => {
            airport.date.buckets.forEach((date)=> {
                date.hour.buckets.forEach((hour)=> {
                    let fields = {};
                    fields["Airport"] = airport.key;
                    fields["LongDate"] = date.key;
                    fields["Date"] = date.key_as_string;
                    fields["Hour"] = hour.key;
                    fields["AvgWait"] = hour._Average.value;
                    fields["MaxWait"] = hour._Max.value;
                    fields["Booths"] = hour._Booths.value;
                    fields["Count"] = hour._Total.value;
                    fields["Lat"] = hour._Lat.value;
                    fields["Lon"] = hour._Lon.value;
                    fw.write(fields);
                });
            });
        });
        return "done";
    }, (err)=> {
        console.log(err);
        return (err)
    });
};


module.exports.generateCsv = function () {
    let perDay = rowsByAirportPerDay();
    let perHour = rowsByAirportPerHour();
};