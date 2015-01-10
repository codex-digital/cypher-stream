'use strict';
var oboe        = require('oboe');
var Readable    = require('stream').Readable;
var PassThrough = require('stream').PassThrough;
var util        = require('util');
var urlParser   = require('url');
var normalize   = require('./normalize-query-statement');

util.inherits(CypherStream, Readable);

function CypherStream(databaseUrl, statements, options, token) {
  Readable.call(this, { objectMode: true });
  statements = normalize(statements).filter(function (statement) {
    if(statement.commit) {
      options.commit = true;
    }
    if(statement.rollback) {
      options.rollback = true;
    }
    return statement.statement;
  });

  // if a rollback is requested before a transactionId is acquired, we can quit early.
  if(options.rollback && !options.transactionId) {
    this.push(null);
    return this;
  }

  var columns;
  var transactionTimeout;
  var self    = this;
  var headers = {
    'X-Stream': true,
    'Accept': 'application/json'
  };
  var currentStatement = 0;
  var callbackStream   = null;

  var parsedUrl = urlParser.parse(databaseUrl);

  // check for the token given by the new server
  if(token == null){
    //add HTTP basic auth if needed
    if (parsedUrl.auth) {
      headers.Authorization = 'Basic ' + new Buffer(parsedUrl.auth).toString('base64');
    }
  }else{
    // add the token given by the neo server
    headers.Authorization = 'Basic realm=\"Neo4j\" ' + new Buffer(':' + token).toString('base64');
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

  // console.log('%s %s', options.transactionId && options.rollback ? 'DELETE': 'POST', url, JSON.stringify(statements));

  var stream = oboe({
    url     : url,
    method  : options.transactionId && options.rollback ? 'DELETE': 'POST',
    headers : headers,
    body    : { statements: statements }
  });

  stream.node('!.*', function CypherStreamAll(){
    return oboe.drop; // discard records as soon as they're processed to conserve memory
  });

  stream.on('start', function CypherStreamStart(status, headers) {
    if (headers.location) {
      self.emit('transactionId', headers.location.split('/').pop());
    }
  });

  stream.node('!transaction.expires', function CypherStreamTransactionExpires(date) {
    clearTimeout(transactionTimeout);
    var timeTillExpired = Date.parse(date)-Date.now();
    transactionTimeout  = setTimeout(transactionExpired, timeTillExpired);
    self.emit('expires', date);
  });

  stream.path('!results[*]', function CypherStreamResult() {
    if (callbackStream) {
      self.unpipe(callbackStream);
      callbackStream.end();
      callbackStream = null;
    }
    if (statements[currentStatement].callback) {
      callbackStream = new PassThrough({ objectMode: true });
      statements[currentStatement].callback(callbackStream);
    }
    currentStatement++;
  });

  stream.node('!results[*].columns', function CypherStreamNodeColumns(c) {
    self.emit('columns', c);
    columns = c;
  });

  stream.node('!results[*].data[*].row', function CypherStreamNodeData(result) {
    var data = {};
    columns.forEach(function (column, i) {
      data[column] = result[i];
    });
    if (callbackStream) {
      callbackStream.write(data);
    }
    self.push(data);
  });

  stream.on('done', function CypherStreamDone() {
    clearTimeout(transactionTimeout);
    if (options && options.commit || options.rollback) {
      self.emit('transactionComplete');
    }
    if (callbackStream) {
      callbackStream.end();
    }
    self.push(null);
  });

  stream.node('!errors[*]', function CypherStreamHandleError(error) {
    var message = 'Query Failure';
    if (error.message) {
      message += ': ' + error.message;
    }
    var err  = new Error(message);
    err.code = error.code;
    self.emit('error', err);
  });

  stream.on('fail', function CypherStreamHandleFailure(error) {
    var err;
    // handle non-neo4j errors
    if (!error.jsonBody) {
      // pass the Error instance through, creating one if necessary
      err = error.thrown || new Error('Neo4j ' + error.statusCode);
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
    err = new Error(message);
    err.neo4j = error.jsonBody;
    err.neo4j.statusCode = statusCode;
    self.emit('error', err);
    self.push(null);
  });

  this._read = function () { };
}

module.exports = CypherStream;
