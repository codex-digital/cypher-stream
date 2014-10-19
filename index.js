var oboe      = require('oboe');
var Readable  = require('stream').Readable;
var Duplex    = require('stream').Duplex;
var util      = require('util');
var urlParser = require('url');

util.inherits(CypherStream, Readable);
util.inherits(TransactionStream, Duplex);

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

function CypherStream (databaseUrl, statement, parameters, options) {
  Readable.call(this, { objectMode: true });

  var columns;
  var _this   = this;
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
  if(options && options.transactionId) {
    url += '/'+options.transactionId;
  }
  if(options && options.commit) {
    url += '/commit';
  }

  function transactionExpired () {
    _this.emit('expired');
    this.push(null);
  }

  var body = { statements: [] };

  if (statement) {
    body.statements = [ { statement: statement, parameters: parameters} ];
  }

  var stream = oboe({
    url     : url,
    method  : 'POST',
    headers : headers,
    body    : body,
  });

  stream.on('start', function (status, headers) {
    if (headers.location) {
      _this.emit('transactionId', headers.location.split('/').pop());
    }
  });

  stream.node('!transaction.expires', function (result, path, ancestors) {
    clearTimeout(transactionTimeout);
    var timeTillExpire = Date.parse('Sun, 19 Oct 2014 05:06:47')-Date.now();
    transactionTimeout = setTimeout(transactionExpired, timeTillExpire);
  });

  stream.node('!results[*].columns', function CypherStreamNodeColumns(c) {
    _this.emit('columns', c);
    columns = c;
  });

  stream.node('!results[*].data[*].row', function CypherStreamNodeData(result, path, ancestors) {
    var data = {};
    columns.forEach(function (column, i) {
      data[column] = extractData(result[i]);
    });
    _this.push(data);
  });

  stream.done(function CypherStreamDone(complete) {
    if(options && options.commit) {
      _this.emit('transactionComplete');
    }
    _this.push(null);
  });

  stream.node('!errors[*]', function (error, path, ancestors) {
    var message = "Query Failure";
    if (error.message) {
      message += ": " + error.message;
    }
    var err = new Error(message);
    err.code = error.code;
    _this.emit('error', err);
  });

  stream.fail(function CypherStreamHandleError(error) {
    // handle non-neo4j errors
    if (!error.jsonBody) {
      // pass the Error instance through, creating one if necessary
      var err = error.thrown || new Error('Neo4j ' + error.statusCode);
      err.statusCode = error.statusCode;
      err.body = error.body;
      err.jsonBody = error.jsonBody;
      _this.emit('error', err);
      _this.push(null);
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
    _this.emit('error', err);
    _this.push(null);
  });

  this._read = function () { };
}

function TransactionStream(url, options) {
  Duplex.call(this, { objectMode: true });
  // this._writableState.objectMode = true;
  // this._readableState.objectMode = true;

  var _this = this;
  var transactionId;

  this.commit = function () {
    this.write({ commit: true });
  };

  this._write = function (statement, encoding, done) {
    if (typeof statement === 'string') {
      statement = { query: statement };
    }
    var options = {};
    if (statement.commit) {
      options.commit = true;
    }
    if (transactionId) {
      options.transactionId = transactionId;
    }

    var stream = new CypherStream(url, statement.query, statement.params, options);

    stream.on('transactionId', function (txId) {
      if (!transactionId) {
        transactionId = txId;
      }
    });

    stream.on('transactionComplete', function () {
      _this.push(null);
    });

    stream.on('data', function (data) {
      _this.push(data);
    });
    stream.on('error', function (errors) {
      _this.emit('error', errors);
    });
    stream.on('end', function () {
      done();
    });

  };
  this._read = function () { };

}

module.exports = function Connection(url) {
  var factory = function CypherStreamFactory(query, params) {
    return new CypherStream(url, query, params, { commit: true });
  };
  factory.transaction = function (options) {
    return new TransactionStream(url, options);
  };
  return factory;
};
