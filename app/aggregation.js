'use strict';

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
                                AvgWait: {
                                    avg: {
                                        field: "Average"
                                    }
                                },
                                MaxWait: {
                                    avg: {
                                        field: "Max"
                                    }
                                },
                                Booths: {
                                    avg: {
                                        field: "Booths"
                                    }
                                },
                                Count: {
                                    sum: {
                                        field: "Total"
                                    }
                                },
                                "0-15": {
                                    sum: {
                                        field: "0-15"
                                    }
                                },
                                "16-30": {
                                    sum: {
                                        field: "16-30"
                                    }
                                },
                                "31-45": {
                                    sum: {
                                        field: "31-45"
                                    }
                                },
                                "46-60": {
                                    sum: {
                                        field: "46-60"
                                    }
                                },
                                "61-90": {
                                    sum: {
                                        field: "61-90"
                                    }
                                },
                                "91-120": {
                                    sum: {
                                        field: "91-120"
                                    }
                                },
                                "121Plus": {
                                    sum: {
                                        field: "121Plus"
                                    }
                                },
                                Flights: {
                                    sum: {
                                        field: "Flights"
                                    }
                                },
                                Excluded: {
                                    sum: {
                                        field: "Excluded"
                                    }
                                },
                                Lat: {
                                    avg: {
                                        field: "lat"
                                    }
                                },
                                Lon: {
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
                for (let key of Object.keys(date)) {
                    if (date[key] instanceof Object) {
                        fields[key] = date[key].value;
                    }
                }
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
                                        AvgWait: {
                                            avg: {
                                                field: "Average"
                                            }
                                        },
                                        MaxWait: {
                                            avg: {
                                                field: "Max"
                                            }
                                        },
                                        Booths: {
                                            avg: {
                                                field: "Booths"
                                            }
                                        },
                                        Count: {
                                            sum: {
                                                field: "Total"
                                            }
                                        },
                                        "0-15": {
                                            sum: {
                                                field: "0-15"
                                            }
                                        },
                                        "16-30": {
                                            sum: {
                                                field: "16-30"
                                            }
                                        },
                                        "31-45": {
                                            sum: {
                                                field: "31-45"
                                            }
                                        },
                                        "46-60": {
                                            sum: {
                                                field: "46-60"
                                            }
                                        },
                                        "61-90": {
                                            sum: {
                                                field: "61-90"
                                            }
                                        },
                                        "91-120": {
                                            sum: {
                                                field: "91-120"
                                            }
                                        },
                                        "121Plus": {
                                            sum: {
                                                field: "121Plus"
                                            }
                                        },
                                        Flights: {
                                            sum: {
                                                field: "Flights"
                                            }
                                        },
                                        Excluded: {
                                            sum: {
                                                field: "Excluded"
                                            }
                                        },
                                        Lat: {
                                            avg: {
                                                field: "lat"
                                            }
                                        },
                                        Lon: {
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
                    for (let key of Object.keys(hour)) {
                        if (hour[key] instanceof Object) {
                            fields[key] = hour[key].value;
                        }
                    }
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