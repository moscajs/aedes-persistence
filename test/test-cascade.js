const { test } = require('node:test')
const memory = require('../persistence.js')
const { CallBackPersistence } = require('../callBackPersistence.js')
const { PromisifiedPersistence } = require('../promisified.js')
const abs = require('../abstract.js')

// test the following:
// abstract.js -> async callback => promisified -> callback memory
// this should prove that its possible to layer anyway we want

const persistence = (opts = {}) => {
  opts.broadcastSubscriptions = true
  const asyncInstanceFactory = (opts) => new PromisifiedPersistence(memory())
  const instance = new CallBackPersistence(asyncInstanceFactory, opts)
  return instance
}

abs({
  test,
  persistence,
  testAsync: true
})
