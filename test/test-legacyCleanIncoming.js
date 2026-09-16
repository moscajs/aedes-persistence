const { test } = require('node:test')
const assert = require('node:assert')
const { CallBackPersistence } = require('../callBackPersistence.js')

// A pre-11 async persistence: every method aedes needs except cleanIncoming.
class LegacyAsyncPersistence {
  async setup (broker) {
    this.broker = broker
    return this
  }
}

class ModernAsyncPersistence extends LegacyAsyncPersistence {
  constructor () {
    super()
    this.cleaned = []
  }

  async cleanIncoming (client) {
    this.cleaned.push(client.id)
  }
}

const wrap = (AsyncImpl) => new CallBackPersistence(() => new AsyncImpl())

const ready = (instance) => new Promise(resolve => {
  instance.once('ready', resolve)
  instance.broker = { id: 'broker-1' }
})

test('a wrapped persistence without cleanIncoming does not advertise it', t => {
  const instance = wrap(LegacyAsyncPersistence)

  // aedes feature-detects with exactly this check; a bare delegation would pass
  // it and then throw synchronously on every clean-session CONNECT.
  assert.strictEqual(typeof instance.cleanIncoming, 'undefined')
})

test('a wrapped persistence with cleanIncoming advertises and delegates it', async t => {
  const instance = wrap(ModernAsyncPersistence)
  assert.strictEqual(typeof instance.cleanIncoming, 'function')

  await ready(instance)

  await instance.cleanIncoming({ id: 'promise-form' })

  const client = await new Promise((resolve, reject) => {
    instance.cleanIncoming({ id: 'callback-form' }, (err, client) => {
      if (err) { reject(err) } else { resolve(client) }
    })
  })
  assert.strictEqual(client.id, 'callback-form')

  assert.deepStrictEqual(instance.asyncPersistence.cleaned, ['promise-form', 'callback-form'])
})

test('a queued cleanIncoming runs once the persistence is ready', async t => {
  const instance = wrap(ModernAsyncPersistence)

  const done = new Promise(resolve => {
    instance.cleanIncoming({ id: 'queued' }, resolve)
  })
  assert.deepStrictEqual(instance.asyncPersistence.cleaned, [])

  instance.broker = { id: 'broker-1' }
  await done
  assert.deepStrictEqual(instance.asyncPersistence.cleaned, ['queued'])
})
