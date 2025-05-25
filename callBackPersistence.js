'use strict'

/* This module provides a callback layer for async persistence implementations */
const Packet = require('aedes-packet')
const { Readable } = require('node:stream')
const { EventEmitter } = require('node:events')

// System topics for subscription management
const TOPIC_ADD_SUBSCRIPTION = '$SYS/sub/add'
const TOPIC_REMOVE_SUBSCRIPTION = '$SYS/sub/rm'
const TOPIC_SUBSCRIPTION_PATTERN = '$SYS/sub/+'
// Constants to increase readability
const SUBSCRIBE = true
const UNSUBSCRIBE = false

function toValue (obj, prop) {
  if (typeof obj === 'object' && obj !== null && prop in obj) {
    return obj[prop]
  }
  return obj
}

function getKey (clientId, isSub, topic) {
  return `${clientId}-${isSub ? 'sub_' : 'unsub_'}${topic || ''}`
}

function subToTopic (sub) {
  return sub.topic
}

function noop () {}

function brokerPublish (broker, topic, clientId, subs, cb) {
  const encoded = JSON.stringify({ clientId, subs })
  const packet = new Packet({
    topic,
    payload: encoded
  })
  broker.publish(packet, cb)
}

class CallBackPersistence extends EventEmitter {
  constructor (asyncInstanceFactory, opts = {}) {
    super()

    this.ready = false
    this.destroyed = false
    this.asyncPersistence = asyncInstanceFactory(opts)
    this.broadcastSubscriptions = opts.broadcastSubscriptions || this.asyncPersistence.broadcastSubscriptions
    this._trie = this.asyncPersistence._trie
  }

  _onMessage (packet, cb) {
    const decoded = JSON.parse(packet.payload)
    const clientId = decoded.clientId
    const isAddSubscription = packet.topic === TOPIC_ADD_SUBSCRIPTION

    for (let i = 0; i < decoded.subs.length; i++) {
      const sub = decoded.subs[i]
      sub.clientId = clientId

      if (isAddSubscription) {
        if (sub.qos > 0) {
          this._trie.add(sub.topic, sub)
        } else {
          this._trie.remove(sub.topic, sub)
        }
      } else if (packet.topic === TOPIC_REMOVE_SUBSCRIPTION) {
        this._trie.remove(sub.topic, sub)
      }
    }

    if (decoded.subs.length > 0) {
      const key = getKey(clientId, isAddSubscription, decoded.subs[0].topic)
      const waiting = this._waiting.get(key)
      if (waiting) {
        this._waiting.delete(key)
        process.nextTick(waiting)
      }
    }
    cb()
  }

  get broker () {
    return this._broker
  }

  set broker (broker) {
    this._broker = broker
    if (this.broadcastSubscriptions) {
      this._waiting = new Map()
      this._onSubMessage = this._onMessage.bind(this)
      this.asyncPersistence._trie = this._trie
      this.broker.subscribe(
        TOPIC_SUBSCRIPTION_PATTERN,
        this._onSubMessage,
        this._setup.bind(this)
      )
    } else {
      this._setup()
    }
  }

  _waitFor (client, isSub, topic, cb) {
    this._waiting.set(getKey(client.id, isSub, topic), cb)
  }

  _addedSubscriptions (client, subs, cb) {
    if (!this.broadcastSubscriptions) {
      return cb(null, client)
    }
    if (subs.length === 0) {
      return cb(null, client)
    }

    let errored = false

    this._waitFor(client, SUBSCRIBE, subs[0].topic, (err) => {
      if (!errored && err) {
        return cb(err)
      }
      if (!errored) {
        cb(null, client)
      }
    })

    brokerPublish(this._broker, TOPIC_ADD_SUBSCRIPTION, client.id, subs, (err) => {
      if (err) {
        errored = true
        cb(err)
      }
    })
  }

  _removedSubscriptions (client, subs, cb) {
    if (!this.broadcastSubscriptions) {
      return cb(null, client)
    }
    let errored = false
    let key = subs

    if (subs.length > 0) {
      key = subs[0]
    }

    this._waitFor(client, UNSUBSCRIBE, key, (err) => {
      if (!errored && err) {
        return cb(err)
      }
      if (!errored) {
        cb(null, client)
      }
    })

    const mappedSubs = subs.map(sub => { return { topic: sub } })
    brokerPublish(this._broker, TOPIC_REMOVE_SUBSCRIPTION, client.id, mappedSubs, (err) => {
      if (err) {
        errored = true
        cb(err)
      }
    })
  }

  subscriptionsByTopic (topic, cb) {
    if (!this.ready) {
      this.once('ready', this.subscriptionsByTopic.bind(this, topic, cb))
      return this
    }
    this.asyncPersistence.subscriptionsByTopic(topic)
      .then(resubs => {
        process.nextTick(cb, null, resubs)
      })
      .catch(cb)
  }

  cleanSubscriptions (client, cb) {
    this.subscriptionsByClient(client, (err, subs, client) => {
      if (err || !subs) {
        return cb(err, client)
      }
      const newSubs = subs.map(subToTopic)
      this.removeSubscriptions(client, newSubs, cb)
    })
  }

  _setup () {
    if (this.ready) {
      return
    }
    this.asyncPersistence.broker = this.broker

    this.asyncPersistence.setup()
      .then(() => {
        this.ready = true
        this.emit('ready')
      })
      .catch(err => {
        this.emit('error', err)
      })
  }

  storeRetained (packet, cb) {
    if (!this.ready) {
      this.once('ready', this.storeRetained.bind(this, packet, cb))
      return
    }
    this.asyncPersistence.storeRetained(packet).then(() => {
      cb(null)
    }).catch(cb)
  }

  createRetainedStream (pattern) {
    return Readable.from(this.asyncPersistence.createRetainedStream(pattern))
  }

  createRetainedStreamCombi (patterns) {
    return Readable.from(this.asyncPersistence.createRetainedStreamCombi(patterns))
  }

  addSubscriptions (client, subs, cb) {
    if (!this.ready) {
      this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
      return
    }
    this.asyncPersistence.addSubscriptions(client, subs)
      .then(() => {
        this._addedSubscriptions(client, subs, () => cb(null, client))
      })
      .catch(err => cb(err, client))
  }

  removeSubscriptions (client, subs, cb) {
    if (!this.ready) {
      this.once('ready', this.removeSubscriptions.bind(this, client, subs, cb))
      return
    }

    this.asyncPersistence.removeSubscriptions(client, subs)
      .then(() => {
        this._removedSubscriptions(client, subs, (err) => cb(err, client))
      })
      .catch(err => cb(err, client))
  }

  subscriptionsByClient (client, cb) {
    if (!this.ready) {
      this.once('ready', this.subscriptionsByClient.bind(this, client, cb))
      return
    }

    this.asyncPersistence.subscriptionsByClient(client)
      .then(results => {
        // promisified shim returns an object, true async only the resubs
        const resubs = results?.resubs || results
        process.nextTick(cb, null, resubs.length > 0 ? resubs : null, client)
      })
      .catch(cb)
  }

  countOffline (cb) {
    this.asyncPersistence.countOffline()
      .then(res => process.nextTick(cb, null, res.subsCount, res.clientsCount))
      .catch(cb)
  }

  destroy (cb = noop) {
    if (!this.ready) {
      this.once('ready', this.destroy.bind(this, cb))
      return
    }

    if (this._destroyed) {
      throw new Error('destroyed called twice!')
    }

    this._destroyed = true

    if (this.broadcastSubscriptions) {
      // Unsubscribe from broker topics
      this.broker.unsubscribe(TOPIC_SUBSCRIPTION_PATTERN, this._onSubMessage, () => {
        this.asyncPersistence.destroy()
          .finally(cb) // swallow err in case of failure
      })
    } else {
      this.asyncPersistence.destroy()
        .finally(cb) // swallow err in case of failure
    }
  }

  outgoingEnqueue (sub, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.outgoingEnqueue.bind(this, sub, packet, cb))
      return
    }
    this.asyncPersistence.outgoingEnqueue(sub, packet)
      .then(() => process.nextTick(cb, null, packet))
      .catch(cb)
  }

  outgoingEnqueueCombi (subs, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.outgoingEnqueueCombi.bind(this, subs, packet, cb))
      return
    }
    this.asyncPersistence.outgoingEnqueueCombi(subs, packet)
      .then(() => process.nextTick(cb, null, packet))
      .catch(cb)
  }

  outgoingStream (client) {
    return Readable.from(this.asyncPersistence.outgoingStream(client))
  }

  outgoingUpdate (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.outgoingUpdate.bind(this, client, packet, cb))
      return
    }
    this.asyncPersistence.outgoingUpdate(client, packet)
      .then(() => cb(null, client, packet))
      .catch(cb)
  }

  outgoingClearMessageId (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.outgoingClearMessageId.bind(this, client, packet, cb))
      return
    }
    this.asyncPersistence.outgoingClearMessageId(client, packet)
      .then((packet) => cb(null, packet))
      .catch(cb)
  }

  incomingStorePacket (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.incomingStorePacket.bind(this, client, packet, cb))
      return
    }
    this.asyncPersistence.incomingStorePacket(client, packet)
      .then(() => cb(null))
      .catch(cb)
  }

  incomingGetPacket (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.incomingGetPacket.bind(this, client, packet, cb))
      return
    }
    this.asyncPersistence.incomingGetPacket(client, packet)
      .then((packet) => cb(null, packet, client))
      .catch(cb)
  }

  incomingDelPacket (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.incomingDelPacket.bind(this, client, packet, cb))
      return
    }
    this.asyncPersistence.incomingDelPacket(client, packet)
      .then(() => cb(null))
      .catch(cb)
  }

  putWill (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.putWill.bind(this, client, packet, cb))
      return
    }
    this.asyncPersistence.putWill(client, packet)
      .then(() => cb(null, client))
      .catch(cb)
  }

  getWill (client, cb) {
    this.asyncPersistence.getWill(client)
      .then((result) => {
        // promisified shim returns an object, true async only the resubs
        const packet = toValue(result, 'packet')
        cb(null, packet, client)
      })
      .catch(cb)
  }

  delWill (client, cb) {
    this.asyncPersistence.delWill(client)
      .then(result => {
        // promisified shim returns an object, true async only the resubs
        const packet = toValue(result, 'packet')
        cb(null, packet, client)
      })
      .catch(cb)
  }

  streamWill (brokers) {
    return Readable.from(this.asyncPersistence.streamWill(brokers))
  }

  getClientList (topic) {
    return Readable.from(this.asyncPersistence.getClientList(topic))
  }
}

module.exports = { CallBackPersistence }
