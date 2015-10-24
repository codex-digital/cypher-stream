'use strict';
var R = require('ramda');

var cond     = R.cond;
var flatten  = R.flatten;
var identity = R.identity;
var is       = R.is;
var isNil    = R.isNil;
var map      = R.map;
var objOf    = R.objOf;
var pipe     = R.pipe;

var normalize = cond([
  [isNil,      identity],
  [is(String), objOf('statement')],
  [is(Object), identity],
]);
/**
 * Standardizes a query statement from various formats to an array of objects
 */
module.exports = pipe(R.of, flatten, map(normalize));
