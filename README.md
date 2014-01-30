# cypher-stream [![Build Status](https://travis-ci.org/brian-gates/cypher-stream.png?branch=master)](https://travis-ci.org/brian-gates/cypher-stream) [![devDependency Status](https://david-dm.org/brian-gates/cypher-stream.png?theme=shields.io)](https://david-dm.org/brian-gates/cypher-stream.png#info=devDependencies)

Neo4j cypher queries as node object streams.

## Installation
```
npm install cypher-stream
```

## Basic usage

``` js
var neo4j_cypher_stream = require('cypher-stream');
var cypher = neo4j_cypher_stream('http://localhost:7474/db/data/cypher');

cypher('match (user:User) return user')
  .on('data', function (result){
    console.log(result.user.first_name);
  })
  .on('end', function() {
    console.log('all done');
  })
;
```
