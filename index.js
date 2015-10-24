'use strict';
var CypherStream      = require('./CypherStream');
var neo4j             = require('neo4j-driver').v1;
var R                 = require('ramda');
var TransactionStream = require('./TransactionStream');
var $ = require('highland');

var all     = R.all;
var compose = R.compose;
var cond    = R.cond;
var isNil   = R.isNil;
var not     = R.not;
var unapply = R.unapply;
var always = R.always;


var notNil    = compose(not, isNil);

// (user, pass) => auth || undefined
var auth = cond([
  [unapply(all(notNil)), neo4j.auth.basic ],
  [R.T,                  always(undefined)],
]);

module.exports = function Connection(url, user, pass) {

  var driver  = neo4j.driver(url || 'bolt://localhost', auth(user, pass));

  var factory = function CypherStreamFactory(statement, parameters) {
    if (parameters) {
      statement = [ { statement, parameters } ];
    }
    var session = driver.session();
    return new CypherStream(session, statement)
    .on('end', () => session.close());
  };

  factory.transaction = options => {
    var session = driver.session();
    return new TransactionStream(session, options)
    .on('end', () => session.close());
  };

  return factory;
};
