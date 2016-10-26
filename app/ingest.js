var fs = require('fs');
var parse = require('csv-parse');
var es = require('./elastic');

exports.getData = function() {
    var rs = fs.createReadStream('/Users/hp/workbench/projects/cbp/wait-times/data/ATL_E.csv');
    parser = parse({columns: true}, function (err, data) {
        for (var i=0;i<data.length;i++) {
            var waittime = data[i];
            es.esClient().create({
                index: 'cbp',
                type: 'wait-times',
                id: waittime.Airport + waittime.Date + waittime.Hour,
                body: waittime
            }, function (error, response) {
                console.log(response);
                console.log(error);
            });
        }
        console.log(data);
    });
    rs.pipe(parser);
};

