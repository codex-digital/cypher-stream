var request                 = require('request');
var Neo4jStreamDeserializer = require('neo4j-stream-deserializer');

module.exports = function(url) {
  return function(query, params) {
    var form = { query: query };
    if(params) {
      form.params = params;
    }
    return request.post({
      url     : url,
      form    : form,
      headers : { "X-Stream": true, "Accept": "application/json" }
    }).pipe(new Neo4jStreamDeserializer());
  }
};