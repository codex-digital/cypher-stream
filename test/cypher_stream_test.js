var should              = require('should');
var cypher              = require('../index')('http://localhost:7474');

describe('Cypher stream', function () {
  before(function (done){
    cypher('FOREACH (x IN range(1,10) | CREATE(:Test {test: true}))')
      .on('end', done)
      .resume();
  });
  after(function (done){
    cypher('MATCH (n:Test) DELETE n')
      .on('end', done)
      .resume();
  });

  it('works', function (done) {
    var results = 0;
    cypher('match (n:Test) return n limit 10')
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

  it('handles errors', function (done) {
    var errored = false;
    cypher('invalid query')
      .on('data', function (data){
        console.log(data);
      })
      .on('error', function (error) {
        errored = true;
        error.statusCode.should.eql(400);
      })
      .on('end', function() {
        errored.should.be.true;
        done();
      })
    ;
  });

});