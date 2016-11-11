var fs = require('fs');
var parse = require('csv-parse');
var es = require('./elastic');
var _ = require('lodash');

exports.getData = new Promise(function(resolve, reject) {
    var rs = fs.createReadStream('/Users/hp/workbench/projects/cbp/wait-times/data/ATL.csv');
    parser = parse({columns: true}, function (err, data) {
        for (var i=0;i<data.length;i++) {
            var waittime = _.omitBy(data[i], _.isEmpty);
            es.esClient().create({
                index: 'cbp',
                type: 'wait-times',
                timeout: -1,
                id: (waittime.Airport + waittime.Terminal + '_' + waittime.Date.replace(/\//g, '_') + '_' + waittime.Hour.replace(/ /g,'')).replace(/ /g,'_'),
                body: waittime
            }, function (error, response) {
                console.log(error);
            });
        }
        resolve('done');
    });
    rs.pipe(parser);
});

