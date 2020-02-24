

import EventEmitter = NodeJS.EventEmitter
import { AedesPacket } from 'aedes-packet'
import { Client } from 'aedes'

declare function persistence(): persistence.MemoryPersistence

// eslint-disable-next-line no-redeclare
declare namespace persistence {
    interface MemoryPersistence {
        storeRetained(packet: AedesPacket, cb: (error: Error | null) => void): void
        createRetainedStream(pattern: String): ReadableStream
        createRetainedStreamCombi(pattern: Array<String>): ReadableStream
        addSubscriptions(client: Client, subs: Array<String>, cb: (error: Error | null, client: Client) => void): void
        removeSubscriptions(client: Client, subs: Array<String>, cb: (error: Error | null, client: Client) => void): void
        subscriptionsByClient(client: Client, cb: (error: Error | null, subs: Array<String>, client: Client) => void): void
        countOffline(cb: (error: Error | null, subscriptionsCount: Number, clientsCount: Number) => void): void
        subscriptionsByTopic(pattern: String, cb: (error: Error | null, subs: Array<String>) => void): void
        cleanSubscriptions(client: Client, cb: (error: Error | null, client: Client) => void): void
        outgoingEnqueue(sub: String, packet: AedesPacket, cb: (error: Error | null) => void): void
        outgoingEnqueueCombi(sub: Array<String>, packet: AedesPacket, cb: (error: Error | null) => void): void
        outgoingUpdate(client: Client, packet: AedesPacket, cb: (error: Error | null, client: Client, packet: AedesPacket) => void): void
        outgoingClearMessageId(client: Client, packet: AedesPacket, cb: (error: Error | null, packet: AedesPacket) => AedesPacket): void
        outgoingStream(client: Client): ReadableStream
        incomingStorePacket(client: Client, packet: AedesPacket, cb: (error: Error | null) => void): void
        incomingGetPacket(client: Client, packet: AedesPacket, cb: (error: Error | null, packet: AedesPacket) => void): void
        incomingDelPacket(client: Client, packet: AedesPacket, cb: (error: Error | null) => void): void
        putWill(client: Client, packet: AedesPacket, cb: (error: Error | null, client: Client) => void): void
        getWill(client: Client, cb: (error: Error | null, will: AedesPacket, client: Client) => void): void
        delWill(client: Client, cb: (error: Error | null, will: AedesPacket, client: Client) => void): void
        streamWill(brokers: Record<String, Number>): ReadableStream
        getClientList(topic: String): ReadableStream
        destroy(cb: (error: Error | null) => void): void
    }
}