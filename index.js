var oboe      = require('oboe');
var Readable  = require('stream').Readable;
var util      = require('util');

util.inherits(CypherStream, Readable);

function CypherStream (url, query, params) {
  Readable.call(this, { objectMode: true });
  var columns;
  var stream = this;
  oboe({
    url     : url+'/db/data/cypher',
    method  : 'POST',
    headers : { "X-Stream": true, "Accept": "application/json" },
    body    : { query: query, params: params  }
  })
  .node('!columns', function (c) {
    stream.emit('columns', c);
    columns = c;
    this.forget();
  })
  .node('!data[*]', function (result, path, ancestors) {
    var data = {};
    columns.forEach(function (column, i) {
      data[column] = result[i].data
    });
    stream.push(data)
  })
  .done(function (complete) {
    stream.push(null);
  })
  .fail(function(error) {
    stream.emit('error', error);
    stream.push(null);
  });

  this._read = function () { }
  return this;
};

module.exports = function Connection(url) {
  return function CypherStreamFactory (query, params) {
    return new CypherStream(url, query, params);
  }
};