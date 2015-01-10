'use strict';
var oboe              = require('oboe');
var CypherStream      = require('./CypherStream');
var TransactionStream = require('./TransactionStream');

module.exports = function Connection(url) {
  var factory = function CypherStreamFactory(query, params) {
    var statements = query;
    if (params) {
      statements = [ { statement: query, parameters: params } ];
    }
    return new CypherStream(url, statements, { commit: true }, factory.AuthToken);
  };
  factory.transaction = function (options) {
    return new TransactionStream(url, options);
  };

  factory.AuthToken = null;
  factory.authorize = function (user, password, callback) {
    callback = callback || function(error, result){};

    // request server auth token
    oboe({
      url: url + "/authentication",
      method: 'POST',
      headers: {
        "X-Stream": true,
        "Accept": "application/json; charset=UTF-8",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        "username": user,
        "password": password
      })
    }).done(function (data) {
      factory.AuthToken = data.authorization_token;

      // check if the password needs to be changed and give error
      var err = null;
      if(data.password_change_required){
        err = {
          code : 403,
          message : "Password change required"
        };
      }

      callback(err, data);
    }).fail(function () {
      // incorrect password (or general failure in the neo server?)
      callback({
        code : 403,
        message : "Incorrect password"
      }, null);
    });
  };

  return factory;
};
