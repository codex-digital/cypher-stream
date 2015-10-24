'use strict';
var $ = require('highland');

// observable => stream
module.exports = observable => $(push =>
  observable.subscribe({
    onNext      : data  => push(null, data),
    onError     : error => { push(error); push(null, $.nil); },
    onCompleted : ()    => push(null, $.nil)
  })
);
