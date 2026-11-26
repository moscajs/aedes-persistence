const { test } = require('node:test')
const memory = require('../persistence.js')
const { getArrayFromStream } = require('../promisified.js')

// abstract.js compares topic sets and cannot see duplicates; check them here.

async function withRetained (topics) {
  const instance = memory()
  await instance.setup({ id: 'broker' })
  for (const topic of topics) {
    await instance.storeRetained({ cmd: 'publish', topic, payload: Buffer.from(topic), qos: 0, retain: true })
  }
  return instance
}

async function topicsOf (stream) {
  return (await getArrayFromStream(stream)).map(p => p.topic).sort()
}

test('overlapping patterns return each retained message once', async (t) => {
  const instance = await withRetained(['a/b', 'a/c', 'x/y'])
  t.after(() => instance.destroy())
  const topics = await topicsOf(instance.createRetainedStreamCombi(['a/b', 'a/+', '#']))
  t.assert.deepStrictEqual(topics, ['a/b', 'a/c', 'x/y'])
})

test('repeated exact patterns return the retained message once', async (t) => {
  const instance = await withRetained(['a/b', 'a/b/c', 'a'])
  t.after(() => instance.destroy())
  const topics = await topicsOf(instance.createRetainedStreamCombi(['a/b', 'nope', 'a/b']))
  t.assert.deepStrictEqual(topics, ['a/b'])
})
