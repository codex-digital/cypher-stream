var oboe      = require('oboe');
var Readable  = require('stream').Readable;
var util      = require('util');

util.inherits(CypherStream, Readable);

// recursively replace each node with its data property if available
function extractData(item) {
  if(!item) {
    return item;
  }
  if(item.data) {
    return extractData(item.data);
  }
  var isArrayOrObject = ['array', 'object'].indexOf(typeof item) !== -1;
  if(!isArrayOrObject) {
    // filter only objects and arrays
    return item;
  }
  // recurse on each property
  Object.keys(item).forEach(function(key){
    item[key] = extractData(item[key]);
  });
  return item;
}

function CypherStream (url, query, params) {
  Readable.call(this, { objectMode: true });
  var columns;
  var stream = this;
  var headers = {
    "X-Stream": true,
    "Accept": "application/json",
  };
  var parsedUrl = require('url').parse(url);

  //add HTTP basic auth if needed
  if(parsedUrl.auth) {
    headers['Authorization'] = 'Basic ' +
               new Buffer(parsedUrl.auth).toString('base64');
  }

  if (url[url.length - 1] !== '/') {
    url += '/';  // ensure trailing slash
  }

  oboe({
    url     : url+'db/data/cypher',
    method  : 'POST',
    headers : headers,
    body    : { query: query, params: params  }
  })
  .node('!columns', function CypherStreamNodeColumns(c) {
    stream.emit('columns', c);
    columns = c;
    this.forget();
  })
  .node('!data[*]', function CypherStreamNodeData(result, path, ancestors) {
    var data = {};
    columns.forEach(function (column, i) {
      data[column] = extractData(result[i]);
    });
    stream.push(data);
  })
  .done(function CypherStreamDone(complete) {
    stream.push(null);
  })
  .fail(function CypherStreamHandleError(error) {
    // handle non-neo4j errors
    if(!error.jsonBody) {
      // pass the Error instance through, creating one if necessary
      var err = error.thrown || new Error('Neo4j ' + error.statusCode);
      err.statusCode = error.statusCode
      err.body = error.body
      err.jsonBody = error.jsonBody;
      stream.emit('error', err);
      stream.push(null);
      return;
    }
    // handle neo4j errors
    var message    = 'Query failure';
    var statusCode = 400;
    if(error.jsonBody.message) {
      message += ': '+error.jsonBody.message;
    }
    if(error.jsonBody.statusCode) {
      statusCode = error.jsonBody.statusCode;
    }
    var err = new Error(message);
    err.neo4j = error.jsonBody;
    err.neo4j.statusCode = statusCode;
    stream.emit('error', err);
    stream.push(null);
  });

  this._read = function () { };
  return this;
}

module.exports = function Connection(url) {
  return function CypherStreamFactory (query, params) {
    return new CypherStream(url, query, params);
  };
};
