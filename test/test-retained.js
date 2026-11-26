const { test } = require('node:test')
const memory = require('../persistence.js')

async function setup (topics) {
  const instance = memory()
  await instance.setup({ id: 'broker' })
  for (const topic of topics) {
    await instance.storeRetained({ cmd: 'publish', topic, payload: Buffer.from(topic), qos: 0, retain: true })
  }
  return instance
}

async function topicsOf (stream) {
  const out = []
  for await (const packet of stream) {
    out.push(packet.topic)
  }
  return out.sort()
}

test('overlapping patterns return each retained message once', async (t) => {
  const instance = await setup(['a/b', 'a/c', 'x/y'])
  t.after(() => instance.destroy())
  const topics = await topicsOf(instance.createRetainedStreamCombi(['a/b', 'a/+', '#']))
  t.assert.deepStrictEqual(topics, ['a/b', 'a/c', 'x/y'])
})

test('exact patterns only match their own topic', async (t) => {
  const instance = await setup(['a/b', 'a/b/c', 'a'])
  t.after(() => instance.destroy())
  const topics = await topicsOf(instance.createRetainedStreamCombi(['a/b', 'nope', 'a/b']))
  t.assert.deepStrictEqual(topics, ['a/b'])
})

test('exact and wildcard patterns combine', async (t) => {
  const instance = await setup(['a/b', 'c/d', 'e/f/g'])
  t.after(() => instance.destroy())
  const topics = await topicsOf(instance.createRetainedStreamCombi(['c/d', 'e/+/g']))
  t.assert.deepStrictEqual(topics, ['c/d', 'e/f/g'])
})
