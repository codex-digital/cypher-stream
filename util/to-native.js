'use strict';
var R     = require('ramda');
var neo4j = require('neo4j-driver').v1;

var both          = R.both;
var compose       = R.compose;
var cond          = R.cond;
var converge      = R.converge;
var has           = R.has;
var identity      = R.identity;
var invoker       = R.invoker;
var is            = R.is;
var isArrayLike   = R.isArrayLike;
var isNil         = R.isNil;
var map           = R.map;
var mapObjIndexed = R.mapObjIndexed;
var prop          = R.prop;
var zipObj        = R.zipObj;

var recordToNative = converge(zipObj, [prop('keys'), prop('_fields')]);
var isRecord       = both(has('_fields'), has('keys'));

// Recursively map Neo4j values
// to their native equivalants
var toNative = cond([
  [isNil,                        identity],
  [is(neo4j.types.Node),         x => compose(toNative, prop('properties'))(x)],
  [is(neo4j.types.Relationship), prop('properties')],
  [neo4j.isInt,                  x => x.inSafeRange() ? x.toNumber() : x.toString()],
  [isArrayLike,                  x => map(toNative, x)],
  [isRecord,                     x => compose(toNative, recordToNative)(x)],
  [is(Object),                   x => mapObjIndexed(toNative, x)],
  [R.T,                          identity],
]);

module.exports = toNative;
