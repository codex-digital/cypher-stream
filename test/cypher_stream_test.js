var neo4j_cypher_stream = require('../index');
var should              = require('should');
var cypher              = neo4j_cypher_stream('http://localhost:9000/db/data/cypher');
var http                = require('http');
var fs                  = require('fs');

describe('Cypher stream', function () {
  var server;
  before(function(){
    server = http.createServer(function (req, res) {
      // simulate neo4j server
      fs.createReadStream('./test/mock/neo4j_response.json').pipe(res);
    }).listen(9000, 'localhost');
  });
  after(function(){
    server.close();
  })

  it('it works', function (done) {
    var results = 0;
    cypher('match (n {test: true}) return n limit 10')
      .on('data', function (result){
        results++;
        result.n.test.should.be.ok;
      })
      .on('end', function() {
        results.should.eql(10);
        done();
      })
    ;
  });

});