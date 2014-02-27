# cypher-stream [![Build Status](https://travis-ci.org/brian-gates/cypher-stream.png?branch=master)](https://travis-ci.org/brian-gates/cypher-stream) [![NPM version](https://badge.fury.io/js/cypher-stream.png)](http://badge.fury.io/js/cypher-stream) [![devDependency Status](https://david-dm.org/brian-gates/cypher-stream.png?theme=shields.io)](https://david-dm.org/brian-gates/cypher-stream.png#info=devDependencies)

Neo4j cypher queries as node object streams.

## Installation
```
npm install cypher-stream
```

## Basic usage

``` js
var cypher = require('cypher-stream')('http://localhost:7474');

cypher('match (user:User) return user')
  .on('data', function (result){
    console.log(result.user.first_name);
  })
  .on('end', function() {
    console.log('all done');
  })
;
```

## Handling errors
``` js
var cypher = require('cypher-stream')('http://localhost:7474');
var should = require('should');
it('handles errors', function (done) {
  var errored = false;
  cypher('invalid query')
    .on('error', function (error) {
      errored = true;
      String(error).should.equal('Error: Query failure: Invalid input \'i\': expected SingleStatement (line 1, column 1)\n"invalid query"\n ^');
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

```