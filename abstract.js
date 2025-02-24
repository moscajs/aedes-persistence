const assert = require('node:assert/strict')
const looseAssert = require('node:assert')
const { Readable } = require('node:stream')
const Packet = require('aedes-packet')

function abstractPersistence (opts) {
  const test = opts.test
  let _persistence = opts.persistence
  const waitForReady = opts.waitForReady

  // requiring it here so it will not error for modules
  // not using the default emitter
  const buildEmitter = opts.buildEmitter || require('mqemitter')

  if (_persistence.length === 0) {
    _persistence = function asyncify (cb) {
      cb(null, opts.persistence())
    }
  }

  function persistence (cb) {
    const mq = buildEmitter()
    const broker = {
      id: 'broker-42',
      mq,
      publish: mq.emit.bind(mq),
      subscribe: mq.on.bind(mq),
      unsubscribe: mq.removeListener.bind(mq),
      counter: 0
    }

    _persistence((err, instance) => {
      if (instance) {
        // Wait for ready event, if applicable, to ensure the persistence isn't
        // destroyed while it's still being set up.
        // https://github.com/mcollina/aedes-persistence-redis/issues/41
        if (waitForReady) {
          // We have to listen to 'ready' before setting broker because that
          // can result in 'ready' being emitted.
          instance.on('ready', () => {
            instance.removeListener('error', cb)
            cb(null, instance)
          })
          instance.on('error', cb)
        }
        instance.broker = broker
        if (waitForReady) {
          // 'ready' event will call back.
          return
        }
      }
      cb(err, instance)
    })
  }

  // legacy third party streams are typically not iterable
  function iterableStream (stream) {
    if (typeof stream[Symbol.asyncIterator] !== 'function') {
      return new Readable({ objectMode: true }).wrap(stream)
    }
    return stream
  }
  // end of legacy third party streams support

  async function getArrayFromStream (stream) {
    const list = []
    for await (const item of iterableStream(stream)) {
      list.push(item)
    }
    return list
  }

  async function streamForEach (stream, fn) {
    for await (const item of iterableStream(stream)) {
      await fn(item)
    }
  }

  function storeRetained (instance, opts, cb) {
    opts = opts || {}

    const packet = {
      cmd: 'publish',
      id: instance.broker.id,
      topic: opts.topic || 'hello/world',
      payload: opts.payload || Buffer.from('muahah'),
      qos: 0,
      retain: true
    }

    instance.storeRetained(packet, err => {
      cb(err, packet)
    })
  }

  function matchRetainedWithPattern (t, pattern, opts) {
    persistence((err, instance) => {
      if (err) { throw err }

      storeRetained(instance, opts, (err, packet) => {
        assert.ok(!err, 'no error')
        let stream
        if (Array.isArray(pattern)) {
          stream = instance.createRetainedStreamCombi(pattern)
        } else {
          stream = instance.createRetainedStream(pattern)
        }

        getArrayFromStream(stream).then(list => {
          assert.deepEqual(list, [packet], 'must return the packet')
          instance.destroy()
        })
      })
    })
  }

  function testInstance (title, cb) {
    test(title, t => {
      persistence((err, instance) => {
        if (err) { throw err }
        cb(t, instance)
      })
    })
  }

  function testPacket (t, packet, expected) {
    if (packet.messageId === null) packet.messageId = undefined
    assert.equal(packet.messageId, undefined, 'should have an unassigned messageId in queue')
    // deepLooseEqual?
    looseAssert.deepEqual(structuredClone(packet), expected, 'must return the packet')
  }

  function deClassed (obj) {
    return Object.assign({}, obj)
  }

  test('store and look up retained messages', t => {
    matchRetainedWithPattern(t, 'hello/world')
  })

  test('look up retained messages with a # pattern', t => {
    matchRetainedWithPattern(t, '#')
  })

  test('look up retained messages with a hello/world/# pattern', t => {
    matchRetainedWithPattern(t, 'hello/world/#')
  })

  test('look up retained messages with a + pattern', t => {
    matchRetainedWithPattern(t, 'hello/+')
  })

  test('look up retained messages with multiple patterns', t => {
    matchRetainedWithPattern(t, ['hello/+', 'other/hello'])
  })

  testInstance('store multiple retained messages in order', (t, instance) => {
    const totalMessages = 1000
    let done = 0

    const retained = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      retain: true
    }

    function checkIndex (index) {
      const packet = new Packet(retained, instance.broker)

      instance.storeRetained(packet, err => {
        assert.ok(!err, 'no error')
        assert.equal(packet.brokerCounter, index + 1, 'packet stored in order')
        if (++done === totalMessages) {
          instance.destroy()
        }
      })
    }

    for (let i = 0; i < totalMessages; i++) {
      checkIndex(i)
    }
  })

  testInstance('remove retained message', (t, instance) => {
    storeRetained(instance, {}, (err, packet) => {
      assert.ok(!err, 'no error')
      storeRetained(instance, {
        payload: Buffer.alloc(0)
      }, err => {
        assert.ok(!err, 'no error')

        const stream = instance.createRetainedStream('#')
        getArrayFromStream(stream).then(list => {
          assert.deepEqual(list, [], 'must return an empty list')
          instance.destroy()
        })
      })
    })
  })

  testInstance('storing twice a retained message should keep only the last', (t, instance) => {
    storeRetained(instance, {}, (err, packet) => {
      assert.ok(!err, 'no error')
      storeRetained(instance, {
        payload: Buffer.from('ahah')
      }, (err, packet) => {
        assert.ok(!err, 'no error')

        const stream = instance.createRetainedStream('#')

        getArrayFromStream(stream).then(list => {
          assert.deepEqual(list, [packet], 'must return the last packet')
          instance.destroy()
        })
      })
    })
  })

  testInstance('Create a new packet while storing a retained message', (t, instance) => {
    const packet = {
      cmd: 'publish',
      id: instance.broker.id,
      topic: opts.topic || 'hello/world',
      payload: opts.payload || Buffer.from('muahah'),
      qos: 0,
      retain: true
    }
    const newPacket = Object.assign({}, packet)

    instance.storeRetained(packet, err => {
      assert.ok(!err, 'no error')
      // packet reference change to check if a new packet is stored always
      packet.retain = false
      const stream = instance.createRetainedStream('#')

      getArrayFromStream(stream).then(list => {
        assert.deepEqual(list, [newPacket], 'must return the last packet')
        instance.destroy()
      })
    })
  })

  testInstance('store and look up subscriptions by client', (t, instance) => {
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'matteo',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'noqos',
      qos: 0,
      rh: 0,
      rap: true,
      nl: false
    }]

    instance.addSubscriptions(client, subs, (err, reClient) => {
      assert.equal(reClient, client, 'client must be the same')
      assert.ok(!err, 'no error')
      instance.subscriptionsByClient(client, (err, resubs, reReClient) => {
        assert.equal(reReClient, client, 'client must be the same')
        assert.ok(!err, 'no error')
        assert.deepEqual(resubs, subs)
        instance.destroy()
      })
    })
  })

  testInstance('remove subscriptions by client', (t, instance) => {
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'matteo',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]

    instance.addSubscriptions(client, subs, (err, reClient) => {
      assert.ok(!err, 'no error')
      instance.removeSubscriptions(client, ['hello'], (err, reClient) => {
        assert.ok(!err, 'no error')
        assert.equal(reClient, client, 'client must be the same')
        instance.subscriptionsByClient(client, (err, resubs, reClient) => {
          assert.equal(reClient, client, 'client must be the same')
          assert.ok(!err, 'no error')
          assert.deepEqual(resubs, [{
            topic: 'matteo',
            qos: 1,
            rh: 0,
            rap: true,
            nl: false
          }])
          instance.destroy()
        })
      })
    })
  })

  testInstance('store and look up subscriptions by topic', (t, instance) => {
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'hello/#',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'matteo',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]

    instance.addSubscriptions(client, subs, err => {
      assert.ok(!err, 'no error')
      instance.subscriptionsByTopic('hello', (err, resubs) => {
        assert.ok(!err, 'no error')
        assert.deepEqual(resubs, [{
          clientId: client.id,
          topic: 'hello/#',
          qos: 1,
          rh: 0,
          rap: true,
          nl: false
        }, {
          clientId: client.id,
          topic: 'hello',
          qos: 1,
          rh: 0,
          rap: true,
          nl: false
        }])
        instance.destroy()
      })
    })
  })

  testInstance('get client list after subscriptions', (t, instance) => {
    const client1 = { id: 'abcde' }
    const client2 = { id: 'efghi' }
    const subs = [{
      topic: 'helloagain',
      qos: 1
    }]

    instance.addSubscriptions(client1, subs, err => {
      assert.ok(!err, 'no error for client 1')
      instance.addSubscriptions(client2, subs, err => {
        assert.ok(!err, 'no error for client 2')
        const stream = instance.getClientList(subs[0].topic)
        getArrayFromStream(stream).then(out => {
          assert.deepEqual(out, [client1.id, client2.id])
          instance.destroy()
        })
      })
    })
  })

  testInstance('get client list after an unsubscribe', (t, instance) => {
    const client1 = { id: 'abcde' }
    const client2 = { id: 'efghi' }
    const subs = [{
      topic: 'helloagain',
      qos: 1
    }]

    instance.addSubscriptions(client1, subs, err => {
      assert.ok(!err, 'no error for client 1')
      instance.addSubscriptions(client2, subs, err => {
        assert.ok(!err, 'no error for client 2')
        instance.removeSubscriptions(client2, [subs[0].topic], (err, reClient) => {
          assert.ok(!err, 'no error for removeSubscriptions')
          const stream = instance.getClientList(subs[0].topic)
          getArrayFromStream(stream).then(out => {
            assert.deepEqual(out, [client1.id])
            instance.destroy()
          })
        })
      })
    })
  })

  testInstance('get subscriptions list after an unsubscribe', (t, instance) => {
    const client1 = { id: 'abcde' }
    const client2 = { id: 'efghi' }
    const subs = [{
      topic: 'helloagain',
      qos: 1
    }]

    instance.addSubscriptions(client1, subs, err => {
      assert.ok(!err, 'no error for client 1')
      instance.addSubscriptions(client2, subs, err => {
        assert.ok(!err, 'no error for client 2')
        instance.removeSubscriptions(client2, [subs[0].topic], (err, reClient) => {
          assert.ok(!err, 'no error for removeSubscriptions')
          instance.subscriptionsByTopic(subs[0].topic, (err, clients) => {
            assert.ok(!err, 'no error getting subscriptions by topic')
            assert.deepEqual(clients[0].clientId, client1.id)
            instance.destroy()
          })
        })
      })
    })
  })

  testInstance('QoS 0 subscriptions, restored but not matched', (t, instance) => {
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 0,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'hello/#',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'matteo',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]

    instance.addSubscriptions(client, subs, err => {
      assert.ok(!err, 'no error')
      instance.subscriptionsByClient(client, (err, resubs) => {
        assert.ok(!err, 'no error')
        assert.deepEqual(resubs, subs)
        instance.subscriptionsByTopic('hello', (err, resubs2) => {
          assert.ok(!err, 'no error')
          assert.deepEqual(resubs2, [{
            clientId: client.id,
            topic: 'hello/#',
            qos: 1,
            rh: 0,
            rap: true,
            nl: false
          }])
          instance.destroy()
        })
      })
    })
  })

  testInstance('clean subscriptions', (t, instance) => {
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1
    }, {
      topic: 'matteo',
      qos: 1
    }]

    instance.addSubscriptions(client, subs, err => {
      assert.ok(!err, 'no error')
      instance.cleanSubscriptions(client, err => {
        assert.ok(!err, 'no error')
        instance.subscriptionsByTopic('hello', (err, resubs) => {
          assert.ok(!err, 'no error')
          assert.deepEqual(resubs, [], 'no subscriptions')

          instance.subscriptionsByClient(client, (err, resubs) => {
            assert.ifError(err)
            assert.deepEqual(resubs, null, 'no subscriptions')

            instance.countOffline((err, subsCount, clientsCount) => {
              assert.ifError(err, 'no error')
              assert.equal(subsCount, 0, 'no subscriptions added')
              assert.equal(clientsCount, 0, 'no clients added')

              instance.destroy()
            })
          })
        })
      })
    })
  })

  testInstance('clean subscriptions with no active subscriptions', (t, instance) => {
    const client = { id: 'abcde' }

    instance.cleanSubscriptions(client, err => {
      assert.ok(!err, 'no error')
      instance.subscriptionsByTopic('hello', (err, resubs) => {
        assert.ok(!err, 'no error')
        assert.deepEqual(resubs, [], 'no subscriptions')

        instance.subscriptionsByClient(client, (err, resubs) => {
          assert.ifError(err)
          assert.deepEqual(resubs, null, 'no subscriptions')

          instance.countOffline((err, subsCount, clientsCount) => {
            assert.ifError(err, 'no error')
            assert.equal(subsCount, 0, 'no subscriptions added')
            assert.equal(clientsCount, 0, 'no clients added')

            instance.destroy()
          })
        })
      })
    })
  })

  testInstance('same topic, different QoS', (t, instance) => {
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 0,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]

    instance.addSubscriptions(client, subs, (err, reClient) => {
      assert.equal(reClient, client, 'client must be the same')
      assert.ifError(err, 'no error')

      instance.subscriptionsByClient(client, (err, subsForClient, client) => {
        assert.ifError(err, 'no error')
        assert.deepEqual(subsForClient, [{
          topic: 'hello',
          qos: 1,
          rh: 0,
          rap: true,
          nl: false
        }])

        instance.subscriptionsByTopic('hello', (err, subsForTopic) => {
          assert.ifError(err, 'no error')
          assert.deepEqual(subsForTopic, [{
            clientId: 'abcde',
            topic: 'hello',
            qos: 1,
            rh: 0,
            rap: true,
            nl: false
          }])

          instance.countOffline((err, subsCount, clientsCount) => {
            assert.ifError(err, 'no error')
            assert.equal(subsCount, 1, 'one subscription added')
            assert.equal(clientsCount, 1, 'one client added')

            instance.destroy()
          })
        })
      })
    })
  })

  testInstance('replace subscriptions', (t, instance) => {
    const client = { id: 'abcde' }
    const topic = 'hello'
    const sub = { topic, rh: 0, rap: true, nl: false }
    const subByTopic = { clientId: client.id, topic, rh: 0, rap: true, nl: false }

    function check (qos, cb) {
      sub.qos = subByTopic.qos = qos
      instance.addSubscriptions(client, [sub], (err, reClient) => {
        assert.equal(reClient, client, 'client must be the same')
        assert.ifError(err, 'no error')
        instance.subscriptionsByClient(client, (err, subsForClient, client) => {
          assert.ifError(err, 'no error')
          assert.deepEqual(subsForClient, [sub])
          instance.subscriptionsByTopic(topic, (err, subsForTopic) => {
            assert.ifError(err, 'no error')
            assert.deepEqual(subsForTopic, qos === 0 ? [] : [subByTopic])
            instance.countOffline((err, subsCount, clientsCount) => {
              assert.ifError(err, 'no error')
              if (qos === 0) {
                assert.equal(subsCount, 0, 'no subscriptions added')
              } else {
                assert.equal(subsCount, 1, 'one subscription added')
              }
              assert.equal(clientsCount, 1, 'one client added')
              cb()
            })
          })
        })
      })
    }

    check(0, () => {
      check(1, () => {
        check(2, () => {
          check(1, () => {
            check(0, () => {
              instance.destroy()
            })
          })
        })
      })
    })
  })

  testInstance('replace subscriptions in same call', (t, instance) => {
    const client = { id: 'abcde' }
    const topic = 'hello'
    const subs = [
      { topic, qos: 0, rh: 0, rap: true, nl: false },
      { topic, qos: 1, rh: 0, rap: true, nl: false },
      { topic, qos: 2, rh: 0, rap: true, nl: false },
      { topic, qos: 1, rh: 0, rap: true, nl: false },
      { topic, qos: 0, rh: 0, rap: true, nl: false }
    ]
    instance.addSubscriptions(client, subs, (err, reClient) => {
      assert.equal(reClient, client, 'client must be the same')
      assert.ifError(err, 'no error')
      instance.subscriptionsByClient(client, (err, subsForClient, client) => {
        assert.ifError(err, 'no error')
        assert.deepEqual(subsForClient, [{ topic, qos: 0, rh: 0, rap: true, nl: false }])
        instance.subscriptionsByTopic(topic, (err, subsForTopic) => {
          assert.ifError(err, 'no error')
          assert.deepEqual(subsForTopic, [])
          instance.countOffline((err, subsCount, clientsCount) => {
            assert.ifError(err, 'no error')
            assert.equal(subsCount, 0, 'no subscriptions added')
            assert.equal(clientsCount, 1, 'one client added')
            instance.destroy()
          })
        })
      })
    })
  })

  testInstance('store and count subscriptions', (t, instance) => {
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1
    }, {
      topic: 'matteo',
      qos: 1
    }, {
      topic: 'noqos',
      qos: 0
    }]

    instance.addSubscriptions(client, subs, (err, reClient) => {
      assert.equal(reClient, client, 'client must be the same')
      assert.ifError(err, 'no error')

      instance.countOffline((err, subsCount, clientsCount) => {
        assert.ifError(err, 'no error')
        assert.equal(subsCount, 2, 'two subscriptions added')
        assert.equal(clientsCount, 1, 'one client added')

        instance.removeSubscriptions(client, ['hello'], (err, reClient) => {
          assert.ifError(err, 'no error')

          instance.countOffline((err, subsCount, clientsCount) => {
            assert.ifError(err, 'no error')
            assert.equal(subsCount, 1, 'one subscription added')
            assert.equal(clientsCount, 1, 'one client added')

            instance.removeSubscriptions(client, ['matteo'], (err, reClient) => {
              assert.ifError(err, 'no error')

              instance.countOffline((err, subsCount, clientsCount) => {
                assert.ifError(err, 'no error')
                assert.equal(subsCount, 0, 'zero subscriptions added')
                assert.equal(clientsCount, 1, 'one client added')

                instance.removeSubscriptions(client, ['noqos'], (err, reClient) => {
                  assert.ifError(err, 'no error')

                  instance.countOffline((err, subsCount, clientsCount) => {
                    assert.ifError(err, 'no error')
                    assert.equal(subsCount, 0, 'zero subscriptions added')
                    assert.equal(clientsCount, 0, 'zero clients added')

                    instance.removeSubscriptions(client, ['noqos'], (err, reClient) => {
                      assert.ifError(err, 'no error')

                      instance.countOffline((err, subsCount, clientsCount) => {
                        assert.ifError(err, 'no error')
                        assert.equal(subsCount, 0, 'zero subscriptions added')
                        assert.equal(clientsCount, 0, 'zero clients added')

                        instance.destroy()
                      })
                    })
                  })
                })
              })
            })
          })
        })
      })
    })
  })

  testInstance('count subscriptions with two clients', (t, instance) => {
    const client1 = { id: 'abcde' }
    const client2 = { id: 'fghij' }
    const subs = [{
      topic: 'hello',
      qos: 1
    }, {
      topic: 'matteo',
      qos: 1
    }, {
      topic: 'noqos',
      qos: 0
    }]

    function remove (client, subs, expectedSubs, expectedClients, cb) {
      instance.removeSubscriptions(client, subs, (err, reClient) => {
        assert.ifError(err, 'no error')
        assert.equal(reClient, client, 'client must be the same')

        instance.countOffline((err, subsCount, clientsCount) => {
          assert.ifError(err, 'no error')
          assert.equal(subsCount, expectedSubs, 'subscriptions added')
          assert.equal(clientsCount, expectedClients, 'clients added')

          cb()
        })
      })
    }

    instance.addSubscriptions(client1, subs, (err, reClient) => {
      assert.equal(reClient, client1, 'client must be the same')
      assert.ifError(err, 'no error')

      instance.addSubscriptions(client2, subs, (err, reClient) => {
        assert.equal(reClient, client2, 'client must be the same')
        assert.ifError(err, 'no error')

        remove(client1, ['foobar'], 4, 2, () => {
          remove(client1, ['hello'], 3, 2, () => {
            remove(client1, ['hello'], 3, 2, () => {
              remove(client1, ['matteo'], 2, 2, () => {
                remove(client1, ['noqos'], 2, 1, () => {
                  remove(client2, ['hello'], 1, 1, () => {
                    remove(client2, ['matteo'], 0, 1, () => {
                      remove(client2, ['noqos'], 0, 0, () => {
                        instance.destroy()
                      })
                    })
                  })
                })
              })
            })
          })
        })
      })
    })
  })

  testInstance('add duplicate subs to persistence for qos > 0', (t, instance) => {
    const client = { id: 'abcde' }
    const topic = 'hello'
    const subs = [{
      topic,
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]

    instance.addSubscriptions(client, subs, (err, reClient) => {
      assert.equal(reClient, client, 'client must be the same')
      assert.ifError(err, 'no error')

      instance.addSubscriptions(client, subs, (err, resCLient) => {
        assert.equal(resCLient, client, 'client must be the same')
        assert.ifError(err, 'no error')
        subs[0].clientId = client.id
        instance.subscriptionsByTopic(topic, (err, subsForTopic) => {
          assert.ifError(err, 'no error')
          assert.deepEqual(subsForTopic, subs)
          instance.destroy()
        })
      })
    })
  })

  testInstance('add duplicate subs to persistence for qos 0', (t, instance) => {
    const client = { id: 'abcde' }
    const topic = 'hello'
    const subs = [{
      topic,
      qos: 0,
      rh: 0,
      rap: true,
      nl: false
    }]

    instance.addSubscriptions(client, subs, (err, reClient) => {
      assert.equal(reClient, client, 'client must be the same')
      assert.ifError(err, 'no error')

      instance.addSubscriptions(client, subs, (err, resCLient) => {
        assert.equal(resCLient, client, 'client must be the same')
        assert.ifError(err, 'no error')
        instance.subscriptionsByClient(client, (err, subsForClient, client) => {
          assert.ifError(err, 'no error')
          assert.deepEqual(subsForClient, subs)
          instance.destroy()
        })
      })
    })
  })

  testInstance('get topic list after concurrent subscriptions of a client', (t, instance) => {
    const client = { id: 'abcde' }
    const subs1 = [{
      topic: 'hello1',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]
    const subs2 = [{
      topic: 'hello2',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]
    let calls = 2

    function done () {
      if (!--calls) {
        instance.subscriptionsByClient(client, (err, resubs) => {
          assert.ok(!err, 'no error')
          resubs.sort((a, b) => b.topic.localeCompare(b.topic, 'en'))
          assert.deepEqual(resubs, [subs1[0], subs2[0]])
          instance.destroy()
        })
      }
    }

    instance.addSubscriptions(client, subs1, err => {
      assert.ok(!err, 'no error for hello1')
      done()
    })
    instance.addSubscriptions(client, subs2, err => {
      assert.ok(!err, 'no error for hello2')
      done()
    })
  })

  testInstance('add outgoing packet and stream it', (t, instance) => {
    const sub = {
      clientId: 'abcde',
      topic: 'hello',
      qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42
    }
    const expected = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      retain: false,
      dup: false,
      brokerId: instance.broker.id,
      brokerCounter: 42,
      messageId: undefined
    }

    instance.outgoingEnqueue(sub, packet, err => {
      assert.ifError(err)
      const stream = instance.outgoingStream(client)

      getArrayFromStream(stream).then(list => {
        const packet = list[0]
        testPacket(t, packet, expected)
        instance.destroy()
      })
    })
  })

  testInstance('add outgoing packet for multiple subs and stream to all', (t, instance) => {
    const sub = {
      clientId: 'abcde',
      topic: 'hello',
      qos: 1
    }
    const sub2 = {
      clientId: 'fghih',
      topic: 'hello',
      qos: 1
    }
    const subs = [sub, sub2]
    const client = {
      id: sub.clientId
    }
    const client2 = {
      id: sub2.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42
    }
    const expected = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      retain: false,
      dup: false,
      brokerId: instance.broker.id,
      brokerCounter: 42,
      messageId: undefined
    }

    instance.outgoingEnqueueCombi(subs, packet, err => {
      assert.ifError(err)
      const stream = instance.outgoingStream(client)
      getArrayFromStream(stream).then(list => {
        const packet = list[0]
        testPacket(t, packet, expected)

        const stream2 = instance.outgoingStream(client2)
        getArrayFromStream(stream2).then(list2 => {
          const packet = list2[0]
          testPacket(t, packet, expected)
          instance.destroy()
        })
      })
    })
  })

  testInstance('add outgoing packet as a string and pump', (t, instance) => {
    const sub = {
      clientId: 'abcde',
      topic: 'hello',
      qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet1 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 10
    }
    const packet2 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('matteo'),
      qos: 1,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 50
    }
    const queue = []
    enqueueAndUpdate(t, instance, client, sub, packet1, 42, updated1 => {
      enqueueAndUpdate(t, instance, client, sub, packet2, 43, updated2 => {
        const stream = instance.outgoingStream(client)

        async function clearQueue (data) {
          return new Promise((resolve, reject) => {
            instance.outgoingUpdate(client, data,
              (err, client, packet) => {
                assert.ok(!err, 'no error')
                queue.push(packet)
                resolve()
              })
          })
        }
        streamForEach(stream, clearQueue).then(function done () {
          assert.equal(queue.length, 2)
          if (queue.length === 2) {
            assert.deepEqual(deClassed(queue[0]), deClassed(updated1))
            assert.deepEqual(deClassed(queue[1]), deClassed(updated2))
          }
          instance.destroy()
        })
      })
    })
  })

  testInstance('add outgoing packet as a string and stream', (t, instance) => {
    const sub = {
      clientId: 'abcde',
      topic: 'hello',
      qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: 'world',
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42
    }
    const expected = {
      cmd: 'publish',
      topic: 'hello',
      payload: 'world',
      qos: 1,
      retain: false,
      dup: false,
      brokerId: instance.broker.id,
      brokerCounter: 42,
      messageId: undefined
    }

    instance.outgoingEnqueueCombi([sub], packet, err => {
      assert.ifError(err)
      const stream = instance.outgoingStream(client)

      getArrayFromStream(stream).then(list => {
        const packet = list[0]
        testPacket(t, packet, expected)
        instance.destroy()
      })
    })
  })

  testInstance('add outgoing packet and stream it twice', (t, instance) => {
    const sub = {
      clientId: 'abcde',
      topic: 'hello',
      qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42,
      messageId: 4242
    }
    const expected = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      retain: false,
      dup: false,
      brokerId: instance.broker.id,
      brokerCounter: 42,
      messageId: undefined
    }

    instance.outgoingEnqueueCombi([sub], packet, err => {
      assert.ifError(err)
      const stream = instance.outgoingStream(client)

      getArrayFromStream(stream).then(list => {
        const packet = list[0]
        testPacket(t, packet, expected)

        const stream2 = instance.outgoingStream(client)

        getArrayFromStream(stream2).then(list2 => {
          const packet = list2[0]
          testPacket(t, packet, expected)
          assert.notEqual(packet, expected, 'packet must be a different object')
          instance.destroy()
        })
      })
    })
  })

  function enqueueAndUpdate (t, instance, client, sub, packet, messageId, callback) {
    instance.outgoingEnqueueCombi([sub], packet, err => {
      assert.ifError(err)
      const updated = new Packet(packet)
      updated.messageId = messageId

      instance.outgoingUpdate(client, updated, (err, reclient, repacket) => {
        assert.ifError(err)
        assert.equal(reclient, client, 'client matches')
        assert.equal(repacket, updated, 'packet matches')
        callback(updated)
      })
    })
  }

  testInstance('add outgoing packet and update messageId', (t, instance) => {
    const sub = {
      clientId: 'abcde', topic: 'hello', qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42
    }

    enqueueAndUpdate(t, instance, client, sub, packet, 42, updated => {
      const stream = instance.outgoingStream(client)
      delete updated.messageId
      getArrayFromStream(stream).then(list => {
        delete list[0].messageId
        assert.notEqual(list[0], updated, 'must not be the same object')
        assert.deepEqual(deClassed(list[0]), deClassed(updated), 'must return the packet')
        assert.equal(list.length, 1, 'must return only one packet')
        instance.destroy()
      })
    })
  })

  testInstance('add 2 outgoing packet and clear messageId', (t, instance) => {
    const sub = {
      clientId: 'abcde', topic: 'hello', qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet1 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42
    }
    const packet2 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('matteo'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 43
    }

    enqueueAndUpdate(t, instance, client, sub, packet1, 42, updated1 => {
      enqueueAndUpdate(t, instance, client, sub, packet2, 43, updated2 => {
        instance.outgoingClearMessageId(client, updated1, (err, packet) => {
          assert.ifError(err)
          assert.deepEqual(packet.messageId, 42, 'must have the same messageId')
          assert.deepEqual(packet.payload.toString(), packet1.payload.toString(), 'must have original payload')
          assert.deepEqual(packet.topic, packet1.topic, 'must have original topic')
          const stream = instance.outgoingStream(client)
          delete updated2.messageId
          getArrayFromStream(stream).then(list => {
            delete list[0].messageId
            assert.notEqual(list[0], updated2, 'must not be the same object')
            assert.deepEqual(deClassed(list[0]), deClassed(updated2), 'must return the packet')
            assert.equal(list.length, 1, 'must return only one packet')
            instance.destroy()
          })
        })
      })
    })
  })

  testInstance('add many outgoing packets and clear messageIds', async (t, instance) => {
    const sub = {
      clientId: 'abcde', topic: 'hello', qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false
    }

    function outStream (instance, client) {
      return iterableStream(instance.outgoingStream(client))
    }

    // we just need a stream to figure out the high watermark
    const stream = outStream(instance, client)
    const total = stream.readableHighWaterMark * 2

    function submitMessage (id) {
      return new Promise((resolve, reject) => {
        const p = new Packet(packet, instance.broker)
        p.messageId = id
        instance.outgoingEnqueue(sub, p, (err) => {
          if (err) {
            return reject(err)
          }
          instance.outgoingUpdate(client, p, resolve)
        })
      })
    }

    function clearMessage (p) {
      return new Promise((resolve, reject) => {
        instance.outgoingClearMessageId(client, p, (err, received) => {
          assert.ifError(err)
          assert.deepEqual(received, p, 'must return the packet')
          resolve()
        })
      })
    }

    for (let i = 0; i < total; i++) {
      await submitMessage(i)
    }

    let queued = 0
    for await (const p of outStream(instance, client)) {
      if (p) {
        queued++
      }
    }
    assert.equal(queued, total, `outgoing queue must hold ${total} items`)

    for await (const p of outStream(instance, client)) {
      await clearMessage(p)
    }

    let queued2 = 0
    for await (const p of outStream(instance, client)) {
      if (p) {
        queued2++
      }
    }
    assert.equal(queued2, 0, 'outgoing queue is empty')
    instance.destroy()
  })

  testInstance('update to publish w/ same messageId', (t, instance) => {
    const sub = {
      clientId: 'abcde', topic: 'hello', qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet1 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 2,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42,
      messageId: 42
    }
    const packet2 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 2,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 50,
      messageId: 42
    }

    instance.outgoingEnqueue(sub, packet1, () => {
      instance.outgoingEnqueue(sub, packet2, () => {
        instance.outgoingUpdate(client, packet1, () => {
          instance.outgoingUpdate(client, packet2, () => {
            const stream = instance.outgoingStream(client)
            getArrayFromStream(stream).then(list => {
              assert.equal(list.length, 2, 'must have two items in queue')
              assert.equal(list[0].brokerCounter, packet1.brokerCounter, 'brokerCounter must match')
              assert.equal(list[0].messageId, packet1.messageId, 'messageId must match')
              assert.equal(list[1].brokerCounter, packet2.brokerCounter, 'brokerCounter must match')
              assert.equal(list[1].messageId, packet2.messageId, 'messageId must match')
              instance.destroy()
            })
          })
        })
      })
    })
  })

  testInstance('update to pubrel', (t, instance) => {
    const sub = {
      clientId: 'abcde', topic: 'hello', qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 2,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42
    }

    instance.outgoingEnqueueCombi([sub], packet, err => {
      assert.ifError(err)
      const updated = new Packet(packet)
      updated.messageId = 42

      instance.outgoingUpdate(client, updated, (err, reclient, repacket) => {
        assert.ifError(err)
        assert.equal(reclient, client, 'client matches')
        assert.equal(repacket, updated, 'packet matches')

        const pubrel = {
          cmd: 'pubrel',
          messageId: updated.messageId
        }

        instance.outgoingUpdate(client, pubrel, err => {
          assert.ifError(err)

          const stream = instance.outgoingStream(client)

          getArrayFromStream(stream).then(list => {
            assert.deepEqual(list, [pubrel], 'must return the packet')
            instance.destroy()
          })
        })
      })
    })
  })

  testInstance('add incoming packet, get it, and clear with messageId', (t, instance) => {
    const client = {
      id: 'abcde'
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 2,
      dup: false,
      length: 14,
      retain: false,
      messageId: 42
    }

    instance.incomingStorePacket(client, packet, err => {
      assert.ifError(err)

      instance.incomingGetPacket(client, {
        messageId: packet.messageId
      }, (err, retrieved) => {
        assert.ifError(err)

        // adjusting the objects so they match
        delete retrieved.brokerCounter
        delete retrieved.brokerId
        delete packet.length
        // strip the class identifier from the packet
        const result = structuredClone(retrieved)
        // Convert Uint8 to Buffer for comparison
        result.payload = Buffer.from(result.payload)
        assert.deepEqual(result, packet, 'retrieved packet must be deeply equal')
        assert.notEqual(retrieved, packet, 'retrieved packet must not be the same object')

        instance.incomingDelPacket(client, retrieved, err => {
          assert.ifError(err)
          instance.incomingGetPacket(client, {
            messageId: packet.messageId
          }, (err, retrieved) => {
            assert.ok(err, 'must error')
            instance.destroy()
          })
        })
      })
    })
  })

  testInstance('store, fetch and delete will message', (t, instance) => {
    const client = {
      id: '12345'
    }
    const expected = {
      topic: 'hello/died',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }

    instance.putWill(client, expected, (err, c) => {
      assert.ifError(err, 'no error')
      assert.equal(c, client, 'client matches')
      instance.getWill(client, (err, packet, c) => {
        assert.ifError(err, 'no error')
        assert.deepEqual(packet, expected, 'will matches')
        assert.equal(c, client, 'client matches')
        client.brokerId = packet.brokerId
        instance.delWill(client, (err, packet, c) => {
          assert.ifError(err, 'no error')
          assert.deepEqual(packet, expected, 'will matches')
          assert.equal(c, client, 'client matches')
          instance.getWill(client, (err, packet, c) => {
            assert.ifError(err, 'no error')
            assert.ok(!packet, 'no will after del')
            assert.equal(c, client, 'client matches')
            instance.destroy()
          })
        })
      })
    })
  })

  testInstance('stream all will messages', (t, instance) => {
    const client = {
      id: '12345',
      brokerId: instance.broker.id
    }
    const toWrite = {
      topic: 'hello/died',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }

    instance.putWill(client, toWrite, (err, c) => {
      assert.ifError(err, 'no error')
      assert.equal(c, client, 'client matches')
      streamForEach(instance.streamWill(), (chunk) => {
        assert.deepEqual(chunk, {
          clientId: client.id,
          brokerId: instance.broker.id,
          topic: 'hello/died',
          payload: Buffer.from('muahahha'),
          qos: 0,
          retain: true
        }, 'packet matches')
        instance.delWill(client, (err, result, client) => {
          assert.ifError(err, 'no error')
          instance.destroy()
        })
      })
    })
  })

  testInstance('stream all will message for unknown brokers', (t, instance) => {
    const originalId = instance.broker.id
    const client = {
      id: '42',
      brokerId: instance.broker.id
    }
    const anotherClient = {
      id: '24',
      brokerId: instance.broker.id
    }
    const toWrite1 = {
      topic: 'hello/died42',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }
    const toWrite2 = {
      topic: 'hello/died24',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }

    instance.putWill(client, toWrite1, (err, c) => {
      assert.ifError(err, 'no error')
      assert.equal(c, client, 'client matches')
      instance.broker.id = 'anotherBroker'
      instance.putWill(anotherClient, toWrite2, (err, c) => {
        assert.ifError(err, 'no error')
        assert.equal(c, anotherClient, 'client matches')
        streamForEach(instance.streamWill({
          anotherBroker: Date.now()
        }), (chunk) => {
          assert.deepEqual(chunk, {
            clientId: client.id,
            brokerId: originalId,
            topic: 'hello/died42',
            payload: Buffer.from('muahahha'),
            qos: 0,
            retain: true
          }, 'packet matches')
          instance.delWill(client, (err, result, client) => {
            assert.ifError(err, 'no error')
            instance.destroy()
          })
        })
      })
    })
  })

  testInstance('delete wills from dead brokers', (t, instance) => {
    const client = {
      id: '42'
    }

    const toWrite1 = {
      topic: 'hello/died42',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }

    instance.putWill(client, toWrite1, (err, c) => {
      assert.ifError(err, 'no error')
      assert.equal(c, client, 'client matches')
      instance.broker.id = 'anotherBroker'
      client.brokerId = instance.broker.id
      instance.delWill(client, (err, result, client) => {
        assert.ifError(err, 'no error')
        instance.destroy()
      })
    })
  })

  testInstance('do not error if unkown messageId in outoingClearMessageId', (t, instance) => {
    const client = {
      id: 'abc-123'
    }

    instance.outgoingClearMessageId(client, 42, err => {
      assert.ifError(err)
      instance.destroy()
    })
  })
}

module.exports = abstractPersistence
