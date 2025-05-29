const { Readable } = require('node:stream')
const QlobberSub = require('qlobber/aedes/qlobber-sub')
const { QlobberTrue } = require('qlobber')
const Packet = require('aedes-packet')
const BroadcastPersistence = require('./broadcastPersistence.js')

const QLOBBER_OPTIONS = {
  wildcard_one: '+',
  wildcard_some: '#',
  separator: '/'
}
const CREATE_ON_EMPTY = true

function * multiIterables (iterables) {
  for (const iter of iterables) {
    yield * iter
  }
}

function * retainedMessagesByPattern (retained, pattern) {
  const qlobber = new QlobberTrue(QLOBBER_OPTIONS)
  qlobber.add(pattern)

  for (const [topic, packet] of retained) {
    if (qlobber.test(topic)) {
      yield packet
    }
  }
}

function * willsByBrokers (wills, brokers) {
  for (const will of wills.values()) {
    if (!brokers[will.brokerId]) {
      yield will
    }
  }
}

function * clientListbyTopic (subscriptions, topic) {
  for (const [clientId, topicMap] of subscriptions) {
    if (topicMap.has(topic)) {
      yield clientId
    }
  }
}

class MemoryPersistence {
  // private class members start with #
  #retained
  #subscriptions
  #outgoing
  #incoming
  #wills
  #clientsCount
  #destroyed
  #broadcastSubscriptions
  #trie
  #broker

  constructor (opts = {}) {
    // using Maps for convenience and security (risk on prototype polution)
    // Map ( topic -> packet )
    this.#retained = new Map()
    // Map ( clientId -> Map( topic -> { qos, rh, rap, nl } ))
    this.#subscriptions = new Map()
    // Map ( clientId  > [ packet ] }
    this.#outgoing = new Map()
    // Map ( clientId -> { packetId -> Packet } )
    this.#incoming = new Map()
    // Map( clientId -> will )
    this.#wills = new Map()
    this.#clientsCount = 0
    this.#destroyed = false
    this.#broadcastSubscriptions = opts.broadcastSubscriptions
    this.#trie = new QlobberSub(QLOBBER_OPTIONS)
  }

  // for testing we need access to the broker
  get broker () {
    return this.#broker
  }

  async setup (broker) {
    this.#broker = broker
    if (this.#broadcastSubscriptions) {
      this.broadcast = new BroadcastPersistence(broker, this.#trie)
      await this.broadcast.brokerSubscribe()
    }
  }

  async storeRetained (pkt) {
    const packet = Object.assign({}, pkt)
    if (packet.payload.length === 0) {
      this.#retained.delete(packet.topic)
    } else {
      this.#retained.set(packet.topic, packet)
    }
  }

  createRetainedStreamCombi (patterns) {
    const iterables = patterns.map((p) => {
      return retainedMessagesByPattern(this.#retained, p)
    })
    return Readable.from(multiIterables(iterables))
  }

  createRetainedStream (pattern) {
    return Readable.from(retainedMessagesByPattern(this.#retained, pattern))
  }

  async addSubscriptions (client, subs) {
    let stored = this.#subscriptions.get(client.id)
    const trie = this.#trie

    if (!stored) {
      stored = new Map()
      this.#subscriptions.set(client.id, stored)
      this.#clientsCount++
    }

    for (const sub of subs) {
      const storedSub = stored.get(sub.topic)
      if (sub.qos > 0) {
        trie.add(sub.topic, {
          clientId: client.id,
          topic: sub.topic,
          qos: sub.qos,
          rh: sub.rh,
          rap: sub.rap,
          nl: sub.nl
        })
      } else if (storedSub?.qos > 0) {
        trie.remove(sub.topic, {
          clientId: client.id,
          topic: sub.topic
        })
      }
      stored.set(sub.topic, { qos: sub.qos, rh: sub.rh, rap: sub.rap, nl: sub.nl })
    }
    if (this.#broadcastSubscriptions) {
      await this.broadcast.addedSubscriptions(client, subs)
    }
  }

  async removeSubscriptions (client, subs) {
    const stored = this.#subscriptions.get(client.id)
    const trie = this.#trie

    if (stored) {
      for (const topic of subs) {
        const storedSub = stored.get(topic)
        if (storedSub !== undefined) {
          if (storedSub.qos > 0) {
            trie.remove(topic, { clientId: client.id, topic })
          }
          stored.delete(topic)
        }
      }

      if (stored.size === 0) {
        this.#clientsCount--
        this.#subscriptions.delete(client.id)
      }
    }
    if (this.#broadcastSubscriptions) {
      await this.broadcast.removedSubscriptions(client, subs)
    }
  }

  async subscriptionsByClient (client) {
    const subs = []
    const stored = this.#subscriptions.get(client.id)
    if (stored) {
      for (const [topic, storedSub] of stored) {
        subs.push({ topic, ...storedSub })
      }
    }
    return subs
  }

  async countOffline () {
    return { subsCount: this.#trie.subscriptionsCount, clientsCount: this.#clientsCount }
  }

  async subscriptionsByTopic (pattern) {
    return this.#trie.match(pattern)
  }

  async cleanSubscriptions (client) {
    const trie = this.#trie
    const stored = this.#subscriptions.get(client.id)

    if (stored) {
      for (const [topic, storedSub] of stored) {
        if (storedSub.qos > 0) {
          trie.remove(topic, { clientId: client.id, topic })
        }
      }

      this.#clientsCount--
      this.#subscriptions.delete(client.id)
    }
  }

  #outgoingEnqueuePerSub (sub, packet) {
    const id = sub.clientId
    const queue = getMapRef(this.#outgoing, id, [], CREATE_ON_EMPTY)
    queue[queue.length] = new Packet(packet)
  }

  async outgoingEnqueue (sub, packet) {
    this.#outgoingEnqueuePerSub(sub, packet)
  }

  async outgoingEnqueueCombi (subs, packet) {
    for (let i = 0; i < subs.length; i++) {
      this.#outgoingEnqueuePerSub(subs[i], packet)
    }
  }

  async outgoingUpdate (client, packet) {
    const outgoing = getMapRef(this.#outgoing, client.id, [], CREATE_ON_EMPTY)

    let temp
    for (let i = 0; i < outgoing.length; i++) {
      temp = outgoing[i]
      if (temp.brokerId === packet.brokerId) {
        if (temp.brokerCounter === packet.brokerCounter) {
          temp.messageId = packet.messageId
          return
        }
        /*
                Maximum of messageId (packet identifier) is 65535 and will be rotated,
                brokerCounter is to ensure the packet identifier be unique.
                The for loop is going to search which packet messageId should be updated
                in the #outgoing queue.
                If there is a case that brokerCounter is different but messageId is same,
                we need to let the loop keep searching
                */
      } else if (temp.messageId === packet.messageId) {
        outgoing[i] = packet
        return
      }
    }
    throw new Error('no such packet')
  }

  async outgoingClearMessageId (client, packet, cb) {
    const outgoing = getMapRef(this.#outgoing, client.id, [], CREATE_ON_EMPTY)

    let temp
    for (let i = 0; i < outgoing.length; i++) {
      temp = outgoing[i]
      if (temp.messageId === packet.messageId) {
        outgoing.splice(i, 1)
        return temp
      }
    }
  }

  outgoingStream (client) {
    // shallow clone the outgoing queue for this client to avoid race conditions
    const outgoing = [].concat(getMapRef(this.#outgoing, client.id, []))
    return Readable.from(outgoing)
  }

  async incomingStorePacket (client, packet) {
    const id = client.id
    const store = getMapRef(this.#incoming, id, {}, CREATE_ON_EMPTY)

    store[packet.messageId] = new Packet(packet)
    store[packet.messageId].messageId = packet.messageId
  }

  async incomingGetPacket (client, packet) {
    const id = client.id
    const store = getMapRef(this.#incoming, id, {})

    this.#incoming.set(id, store)

    if (!store[packet.messageId]) {
      throw new Error('no such packet')
    }
    return store[packet.messageId]
  }

  async incomingDelPacket (client, packet) {
    const id = client.id
    const store = getMapRef(this.#incoming, id, {})
    const toDelete = store[packet.messageId]

    if (!toDelete) {
      throw new Error('no such packet')
    }
    delete store[packet.messageId]
  }

  async putWill (client, packet) {
    packet.brokerId = this.#broker.id
    packet.clientId = client.id
    this.#wills.set(client.id, packet)
  }

  async getWill (client) {
    return this.#wills.get(client.id)
  }

  async delWill (client) {
    const will = this.#wills.get(client.id)
    this.#wills.delete(client.id)
    return will
  }

  streamWill (brokers = {}) {
    return Readable.from(willsByBrokers(this.#wills, brokers))
  }

  getClientList (topic) {
    return Readable.from(clientListbyTopic(this.#subscriptions, topic))
  }

  async destroy () {
    if (this.#destroyed) {
      throw new Error('destroyed called twice!')
    }
    this.#destroyed = true
    if (this.#broadcastSubscriptions) {
      await this.broadcast.brokerUnsubscribe()
    }
    this.#retained = null
  }
}

function getMapRef (map, key, ifEmpty, createOnEmpty = false) {
  const value = map.get(key)
  if (value === undefined && createOnEmpty) {
    map.set(key, ifEmpty)
  }
  return value || ifEmpty
}

module.exports = MemoryPersistence
module.exports.Packet = Packet
