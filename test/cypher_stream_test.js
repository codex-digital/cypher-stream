var neo4j_cypher_stream = require('../index');
var should              = require('should');
var cypher              = neo4j_cypher_stream('http://localhost:7474/db/data/cypher');

describe('Cypher stream', function () {

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