var should              = require('should');
var cypher              = require('../index')('http://localhost:7474');

describe('Cypher stream', function () {
  before(function (done){
    this.timeout(3000); // sometimes travis ci takes too long here
    cypher('FOREACH (x IN range(1,10) | CREATE(:Test {test: true}))')
      .on('end', done)
      .on('error', function (error){
        console.error(error);
      })
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
      .on('error', function (error) {
        errored = true;
        String(error).should.equal('Error: Query failure: Invalid input \'i\': expected <init> (line 1, column 1)\n"invalid query"\n ^');
        error.neo4j.exception.should.equal('SyntaxException');
        error.neo4j.stacktrace.should.be.an.array;
        error.neo4j.statusCode.should.equal(400);
      })
      .on('end', function() {
        errored.should.be.true;
        done();
      })
      .resume() // need to manually start it since we have no on('data')
    ;
  });

  it('handles non-neo4j errors', function (done) {
    var errored = false;
    var expectedError = new Error('Test');
    cypher('match (n:Test) return n limit 1')
      .on('data', function () {
        throw expectedError;
      })
      .on('error', function (error) {
        errored = true;
        error.thrown.should.equal(expectedError);
      })
      .on('end', function() {
        errored.should.be.true;
        done();
      })
      .resume() // need to manually start it since we have no on('data')
    ;
  });

  it('returns non-object values', function (done) {
    cypher('match (n:Test) return n.test as test limit 1')
      .on('data', function (result) {
        result.test.should.be.true;
      })
      .on('end', done)
    ;
  });

  it('returns collections', function (done) {
    cypher('match (n:Test) return collect(n) as nodes limit 1')
      .on('data', function (result) {
        result.nodes[0].test.should.be.true;
      })
      .on('end', done)
    ;
  });

  it('returns non-node collections', function (done) {
    cypher('match (n:Test) return labels(n) as labels limit 1')
      .on('data', function (result) {
        result.labels[0].should.equal('Test');
      })
      .on('end', done)
    ;
  });

});
