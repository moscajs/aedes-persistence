'use strict'

/* This module provides a callback layer for async persistence implementations */
const { Readable } = require('node:stream')
const { EventEmitter } = require('node:events')

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

  async setup (broker) {
    return this.asyncPersistence.setup(broker)
  }

  subscriptionsByTopic (topic, cb) {
    if (cb) {
      if (!this.ready) {
        this.once('ready', this.subscriptionsByTopic.bind(this, topic, cb))
        return this
      }
      this.asyncPersistence.subscriptionsByTopic(topic)
        .then(resubs => {
          process.nextTick(cb, null, resubs)
        })
        .catch(cb)
    } else {
      return this.asyncPersistence.subscriptionsByTopic(topic)
    }
  }

  cleanSubscriptions (client, cb) {
    if (cb) {
      this.asyncPersistence.cleanSubscriptions(client)
        .then(client => {
          process.nextTick(cb, null, client)
        })
        .catch(cb)
    } else {
      return this.asyncPersistence.cleanSubscriptions(client)
    }
  }

  storeRetained (packet, cb) {
    if (cb) {
      if (!this.ready) {
        this.once('ready', this.storeRetained.bind(this, packet, cb))
        return
      }
      this.asyncPersistence.storeRetained(packet).then(() => {
        cb(null)
      }).catch(cb)
    } else {
      return this.asyncPersistence.storeRetained(packet)
    }
  }

  createRetainedStream (pattern) {
    return Readable.from(this.asyncPersistence.createRetainedStream(pattern))
  }

  createRetainedStreamCombi (patterns) {
    return Readable.from(this.asyncPersistence.createRetainedStreamCombi(patterns))
  }

  addSubscriptions (client, subs, cb) {
    if (cb) {
      if (!this.ready) {
        this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
        return
      }
      this.asyncPersistence.addSubscriptions(client, subs)
        .then(() => cb(null, client))
        .catch(cb)
    } else {
      return this.asyncPersistence.addSubscriptions(client, subs)
    }
  }

  removeSubscriptions (client, subs, cb) {
    if (cb) {
      if (!this.ready) {
        this.once('ready', this.removeSubscriptions.bind(this, client, subs, cb))
        return
      }

      this.asyncPersistence.removeSubscriptions(client, subs)
        .then(() => cb(null, client))
        .catch(cb)
    } else {
      return this.asyncPersistence.removeSubscriptions(client, subs)
    }
  }

  subscriptionsByClient (client, cb) {
    if (cb) {
      if (!this.ready) {
        this.once('ready', this.subscriptionsByClient.bind(this, client, cb))
        return
      }

      this.asyncPersistence.subscriptionsByClient(client)
        .then(resubs => {
          process.nextTick(cb, null, resubs.length > 0 ? resubs : null, client)
        })
        .catch(cb)
    } else {
      return this.asyncPersistence.subscriptionsByClient(client)
    }
  }

  countOffline (cb) {
    if (cb) {
      this.asyncPersistence.countOffline()
        .then(res => process.nextTick(cb, null, res.subsCount, res.clientsCount))
        .catch(cb)
    } else {
      return this.asyncPersistence.countOffline()
    }
  }

  destroy (cb) {
    if (cb) {
      if (!this.ready) {
        this.once('ready', this.destroy.bind(this, cb))
        return
      }
      this.asyncPersistence.destroy()
        .finally(cb) // swallow err in case of failure
    } else {
      return this.asyncPersistence.destroy()
    }
  }

  outgoingEnqueue (sub, packet, cb) {
    if (cb) {
      if (!this.ready) {
        this.once('ready', this.outgoingEnqueue.bind(this, sub, packet, cb))
        return
      }
      this.asyncPersistence.outgoingEnqueue(sub, packet)
        .then(() => process.nextTick(cb, null, packet))
        .catch(cb)
    } else {
      return this.asyncPersistence.outgoingEnqueue(sub, packet)
    }
  }

  outgoingEnqueueCombi (subs, packet, cb) {
    if (cb) {
      if (!this.ready) {
        this.once('ready', this.outgoingEnqueueCombi.bind(this, subs, packet, cb))
        return
      }
      this.asyncPersistence.outgoingEnqueueCombi(subs, packet)
        .then(() => process.nextTick(cb, null, packet))
        .catch(cb)
    } else {
      return this.asyncPersistence.outgoingEnqueueCombi(subs, packet)
    }
  }

  outgoingStream (client) {
    return Readable.from(this.asyncPersistence.outgoingStream(client))
  }

  outgoingUpdate (client, packet, cb) {
    if (cb) {
      if (!this.ready) {
        this.once('ready', this.outgoingUpdate.bind(this, client, packet, cb))
        return
      }
      this.asyncPersistence.outgoingUpdate(client, packet)
        .then(() => cb(null, client, packet))
        .catch(cb)
    } else {
      return this.asyncPersistence.outgoingUpdate(client, packet)
    }
  }

  outgoingClearMessageId (client, packet, cb) {
    if (cb) {
      if (!this.ready) {
        this.once('ready', this.outgoingClearMessageId.bind(this, client, packet, cb))
        return
      }
      this.asyncPersistence.outgoingClearMessageId(client, packet)
        .then((packet) => cb(null, packet))
        .catch(cb)
    } else {
      return this.asyncPersistence.outgoingClearMessageId(client, packet)
    }
  }

  incomingStorePacket (client, packet, cb) {
    if (cb) {
      if (!this.ready) {
        this.once('ready', this.incomingStorePacket.bind(this, client, packet, cb))
        return
      }
      this.asyncPersistence.incomingStorePacket(client, packet)
        .then(() => cb(null))
        .catch(cb)
    } else {
      return this.asyncPersistence.incomingStorePacket(client, packet)
    }
  }

  incomingGetPacket (client, packet, cb) {
    if (cb) {
      if (!this.ready) {
        this.once('ready', this.incomingGetPacket.bind(this, client, packet, cb))
        return
      }
      this.asyncPersistence.incomingGetPacket(client, packet)
        .then((packet) => cb(null, packet, client))
        .catch(cb)
    } else {
      return this.asyncPersistence.incomingGetPacket(client, packet)
    }
  }

  incomingDelPacket (client, packet, cb) {
    if (cb) {
      if (!this.ready) {
        this.once('ready', this.incomingDelPacket.bind(this, client, packet, cb))
        return
      }
      this.asyncPersistence.incomingDelPacket(client, packet)
        .then(() => cb(null))
        .catch(cb)
    } else {
      return this.asyncPersistence.incomingDelPacket(client, packet)
    }
  }

  putWill (client, packet, cb) {
    if (cb) {
      if (!this.ready) {
        this.once('ready', this.putWill.bind(this, client, packet, cb))
        return
      }
      this.asyncPersistence.putWill(client, packet)
        .then(() => cb(null, client))
        .catch(cb)
    } else {
      return this.asyncPersistence.putWill(client, packet)
    }
  }

  getWill (client, cb) {
    if (cb) {
      this.asyncPersistence.getWill(client)
        .then(packet => {
          cb(null, packet, client)
        })
        .catch(cb)
    } else {
      return this.asyncPersistence.getWill(client)
    }
  }

  delWill (client, cb) {
    if (cb) {
      this.asyncPersistence.delWill(client)
        .then(packet => {
          cb(null, packet, client)
        })
        .catch(cb)
    } else {
      return this.asyncPersistence.delWill(client)
    }
  }

  streamWill (brokers) {
    return Readable.from(this.asyncPersistence.streamWill(brokers))
  }

  getClientList (topic) {
    return Readable.from(this.asyncPersistence.getClientList(topic))
  }
}

module.exports = { CallBackPersistence }
