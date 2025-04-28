// promisified versions of the persistence interface
// to avoid deep callbacks while testing

class PromisifiedPersistence {
  constructor (instance) {
    this.instance = instance
  }

  get broker () {
    return this.instance.broker
  }

  /* c8 ignore next 3 */
  set broker (newValue) {
    this.instance.broker = newValue
  }

  storeRetained (packet) {
    return new Promise((resolve, reject) => {
      this.instance.storeRetained(packet, err => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  createRetainedStreamCombi (patterns) {
    return this.instance.createRetainedStreamCombi(patterns)
  }

  createRetainedStream (pattern) {
    return this.instance.createRetainedStream(pattern)
  }

  async addSubscriptions (client, subs) {
    return new Promise((resolve, reject) => {
      this.instance.addSubscriptions(client, subs, (err, reClient) => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve(reClient)
        }
      })
    })
  }

  async removeSubscriptions (client, subs) {
    return new Promise((resolve, reject) => {
      this.instance.removeSubscriptions(client, subs, (err, reClient) => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve(reClient)
        }
      })
    })
  }

  async subscriptionsByClient (client) {
    return new Promise((resolve, reject) => {
      this.instance.subscriptionsByClient(client, (err, resubs, reClient) => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve({ resubs, reClient })
        }
      })
    })
  }

  async subscriptionsByTopic (topic) {
    return new Promise((resolve, reject) => {
      this.instance.subscriptionsByTopic(topic, (err, resubs) => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve(resubs)
        }
      })
    })
  }

  async cleanSubscriptions (client) {
    return new Promise((resolve, reject) => {
      this.instance.cleanSubscriptions(client, (err) => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  async countOffline () {
    return new Promise((resolve, reject) => {
      this.instance.countOffline((err, subsCount, clientsCount) => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve({ subsCount, clientsCount })
        }
      })
    })
  }

  async outgoingEnqueue (sub, packet) {
    return new Promise((resolve, reject) => {
      this.instance.outgoingEnqueue(sub, packet, err => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  async outgoingEnqueueCombi (subs, packet) {
    return new Promise((resolve, reject) => {
      this.instance.outgoingEnqueueCombi(subs, packet, err => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  async outgoingClearMessageId (client, packet) {
    return new Promise((resolve, reject) => {
      this.instance.outgoingClearMessageId(client, packet, (err, repacket) => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve(repacket)
        }
      })
    })
  }

  async outgoingUpdate (client, packet) {
    return new Promise((resolve, reject) => {
      this.instance.outgoingUpdate(client, packet, (err, reclient, repacket) => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve({ reclient, repacket })
        }
      })
    })
  }

  outgoingStream (client) {
    return this.instance.outgoingStream(client)
  }

  async incomingStorePacket (client, packet) {
    return new Promise((resolve, reject) => {
      this.instance.incomingStorePacket(client, packet, err => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  async incomingGetPacket (client, packet) {
    return new Promise((resolve, reject) => {
      this.instance.incomingGetPacket(client, packet, (err, retrieved) => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve(retrieved)
        }
      })
    })
  }

  async incomingDelPacket (client, packet) {
    return new Promise((resolve, reject) => {
      this.instance.incomingDelPacket(client, packet, err => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  async putWill (client, packet) {
    return new Promise((resolve, reject) => {
      this.instance.putWill(client, packet, (err, reClient) => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve(reClient)
        }
      })
    })
  }

  async getWill (client) {
    return new Promise((resolve, reject) => {
      this.instance.getWill(client, (err, packet, reClient) => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve({ packet, reClient })
        }
      })
    })
  }

  async delWill (client) {
    return new Promise((resolve, reject) => {
      this.instance.delWill(client, (err, packet, reClient) => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve({ packet, reClient })
        }
      })
    })
  }

  streamWill (brokers) {
    return this.instance.streamWill(brokers)
  }

  getClientList (topic) {
    return this.instance.getClientList(topic)
  }

  async destroy () {
    return new Promise((resolve, reject) => {
      this.instance.destroy((err) => {
        /* c8 ignore next 2 */
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }
}
// end of promisified versions ofthis.instance methods

// helper functions
function waitForEvent (obj, resolveEvt) {
  return new Promise((resolve, reject) => {
    obj.once(resolveEvt, () => {
      resolve()
    })
    obj.once('error', reject)
  })
}

// stream.toArray() sometimes returns undefined or [undefined] instead of []
async function getArrayFromStream (stream) {
  const list = []
  for await (const item of stream) {
    if (item !== undefined && item !== null) {
      list.push(item)
    }
  }
  return list
}

module.exports = {
  PromisifiedPersistence,
  waitForEvent,
  getArrayFromStream
}
