# cypher-stream [![NPM version](https://badge.fury.io/js/cypher-stream.png)](http://badge.fury.io/js/cypher-stream) [![devDependency Status](https://david-dm.org/brian-gates/cypher-stream.png?theme=shields.io)](https://david-dm.org/brian-gates/cypher-stream.png#info=devDependencies)

Neo4j cypher queries as node object streams.

The majority of magic happens in the deserializer, which can be found here:

https://github.com/brian-gates/neo4j-stream-deserializer

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
cypher('invalid query')
  .on('data', function (data){
    console.log(data); // never called
  })
  .on('error', function (error) {
    console.log(error.statusCode); // 400
  })
  .on('end', function() {
    console.log('all done');
  })
;
});

```