var oboe      = require('oboe');
var Readable  = require('stream').Readable;
var util      = require('util');
var urlParser = require('url');

util.inherits(CypherStream, Readable);

// recursively replace each node with its data property if available
function extractData(item) {
  if (!item) {
    return item;
  }
  if (item.data) {
    return extractData(item.data);
  }
  var isArrayOrObject = ['array', 'object'].indexOf(typeof item) !== -1;
  if (!isArrayOrObject) {
    // filter only objects and arrays
    return item;
  }
  // recurse on each property
  Object.keys(item).forEach(function (key) {
    item[key] = extractData(item[key]);
  });
  return item;
}

function CypherStream(databaseUrl, statements, options) {
  Readable.call(this, { objectMode: true });

  // if a rollback is requested before a transactionId is acquired, we can just stop here.
  if(!options.transactionId && options.rollback) {
    this.push(null);
    return this;
  }

  // Normalize various statement syntaxes to [ { statement: statement, parameters: parameters }]
  // { statement: "statement" }
  if (!(statements instanceof Array) && typeof statements === 'object') {
    statements = [statements];
  }
  // "statement"
  if (typeof statements === 'string') {
    statements = [ { statement: statements } ];
  }
  // ["statement"]
  if (statements instanceof Array && typeof statements[0] === 'string') {
    statements = statements.map(function (statement) {
      return { statement: statement };
    });
  }
  if (!(statements instanceof Array) && !options.commit && !options.rollback) {
    throw new Error('CypherStream: No statement or commit/rollback request received.');
  }

  var columns;
  var self    = this;
  var headers = {
    "X-Stream": true,
    "Accept": "application/json",
  };

  var transactionTimeout;

  var parsedUrl = urlParser.parse(databaseUrl);

  //add HTTP basic auth if needed
  if (parsedUrl.auth) {
    headers['Authorization'] = 'Basic ' +
               new Buffer(parsedUrl.auth).toString('base64');
  }

  if (databaseUrl[databaseUrl.length - 1] !== '/') {
    databaseUrl += '/';  // ensure trailing slash
  }
  var url = databaseUrl+'db/data/transaction';
  if (options && options.transactionId) {
    url += '/'+options.transactionId;
  }
  if (options && options.commit) {
    url += '/commit';
  }

  function transactionExpired () {
    self.emit('transactionExpired');
  }

  // console.log("%s %s %s", options.transactionId && options.rollback ? 'DELETE': 'POST', url, statements && statements[0])

  var stream = oboe({
    url     : url,
    method  : options.transactionId && options.rollback ? 'DELETE': 'POST',
    headers : headers,
    body    : statements ? { statements: statements } : null,
  });

  stream.on('start', function (status, headers) {
    if (headers.location) {
      self.emit('transactionId', headers.location.split('/').pop());
    }
  });

  stream.node('!transaction.expires', function (date, path, ancestors) {
    clearTimeout(transactionTimeout);
    var transactionTimeout = setTimeout(transactionExpired, Date.parse(date)-Date.now());
    self.emit('expires', date);
  });

  stream.node('!results[*].columns', function CypherStreamNodeColumns(c) {
    self.emit('columns', c);
    columns = c;
  });

  stream.node('!results[*].data[*].row', function CypherStreamNodeData(result, path, ancestors) {
    var data = {};
    columns.forEach(function (column, i) {
      data[column] = extractData(result[i]);
    });
    self.push(data);
  });

  stream.done(function CypherStreamDone(complete) {
    clearTimeout(transactionTimeout);
    if (options && options.commit || options.rollback) {
      self.emit('transactionComplete');
    }
    self.push(null);
  });

  stream.node('!errors[*]', function (error, path, ancestors) {
    var message = "Query Failure";
    if (error.message) {
      message += ": " + error.message;
    }
    var err = new Error(message);
    err.code = error.code;
    self.emit('error', err);
  });

  stream.fail(function CypherStreamHandleError(error) {
    // handle non-neo4j errors
    if (!error.jsonBody) {
      // pass the Error instance through, creating one if necessary
      var err = error.thrown || new Error('Neo4j ' + error.statusCode);
      err.statusCode = error.statusCode;
      err.body = error.body;
      err.jsonBody = error.jsonBody;
      self.emit('error', err);
      self.push(null);
      return;
    }
    // handle neo4j errors
    var message    = 'Query failure';
    var statusCode = 400;
    if (error.jsonBody.message) {
      message += ': '+error.jsonBody.message;
    }
    if (error.jsonBody.statusCode) {
      statusCode = error.jsonBody.statusCode;
    }
    var err = new Error(message);
    err.neo4j = error.jsonBody;
    err.neo4j.statusCode = statusCode;
    self.emit('error', err);
    self.push(null);
  });

  this._read = function () { };
}

module.exports = CypherStream;
