test:
	./node_modules/.bin/mocha -b

test-coveralls:
	./node_modules/.bin/istanbul cover \
	./node_modules/mocha/bin/_mocha --report lcovonly -- -R spec && \
		cat ./coverage/lcov.info | ./bin/coveralls.js --verbose

.PHONY: test
