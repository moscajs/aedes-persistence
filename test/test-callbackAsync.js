const { test } = require('node:test')
const memory = require('../persistence.js')
const abs = require('../abstract.js')

const persistence = (opts = {}) => {
  opts.broadcastSubscriptions = true
  return memory(opts)
}

// test the async interface of callbackPersistence including broadcastSubscription
abs({
  test,
  persistence,
  testAsync: true
})
