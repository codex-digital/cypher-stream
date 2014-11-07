var CypherStream      = require('./CypherStream');
var TransactionStream = require('./TransactionStream');

module.exports = function Connection(url) {
  var factory = function CypherStreamFactory(query, params) {
    var statements = query;
    if (params) {
      statements = [ { statement: query, parameters: params } ];
    }
    return new CypherStream(url, statements, { commit: true });
  };
  factory.transaction = function (options) {
    return new TransactionStream(url, options);
  };
  return factory;
};
