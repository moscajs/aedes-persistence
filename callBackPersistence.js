'use strict'

/* This module provides a callback layer for async persistence implementations */
const { Readable } = require('node:stream')
const { EventEmitter } = require('node:events')

function toValue (obj, prop) {
  if (typeof obj === 'object' && obj !== null && prop in obj) {
    return obj[prop]
  }
  return obj
}

function subToTopic (sub) {
  return sub.topic
}

function noop () {}

class CallBackPersistence extends EventEmitter {
  constructor (asyncInstanceFactory, opts = {}) {
    super()

    this.ready = false
    this.asyncPersistence = asyncInstanceFactory(opts)
  }

  get broker () {
    return this.asyncPersistence.broker
  }

  set broker (broker) {
    this._setup(broker)
  }

  _setup (broker) {
    if (this.ready) {
      return
    }

    this.asyncPersistence.setup(broker)
      .then(() => {
        this.ready = true
        this.emit('ready')
      })
      .catch(err => {
        this.emit('error', err)
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
      .then(() => cb(null, client))
      .catch(err => cb(err, client))
  }

  removeSubscriptions (client, subs, cb) {
    if (!this.ready) {
      this.once('ready', this.removeSubscriptions.bind(this, client, subs, cb))
      return
    }

    this.asyncPersistence.removeSubscriptions(client, subs)
      .then(() => cb(null, client))
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
        const resubs = toValue(results, 'resubs')
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
    this.asyncPersistence.destroy()
      .finally(cb) // swallow err in case of failure
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
