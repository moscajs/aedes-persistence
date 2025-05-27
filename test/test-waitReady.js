const { test } = require('node:test')
const memory = require('../persistence.js')
const abs = require('../abstract.js')

// test wait for ready, include broadcastSubscription as well
const persistence = (opts = {}) => {
  opts.broadcastSubscriptions = true
  return memory(opts)
}

abs({
  test,
  persistence,
  waitForReady: true
})
