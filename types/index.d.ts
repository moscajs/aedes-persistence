import type { Client } from 'aedes';
import type { AedesPacket } from 'aedes-packet';
import type { Readable } from 'stream';

export type { AedesPacket as Packet } from 'aedes-packet';

export interface AedesPersistenceSubscription {
  clientId?: string;
  topic: string;
  qos: number;
}

export interface Brokers {
  [brokerId: string]: {
    brokerId: string;
  };
}

interface WillPacket extends AedesPacket {
  [key: string]: any;
}

interface Incoming {
  [clientId: string]: { [messageId: string]: AedesPacket };
}

declare class AedesMemoryPersistence {
  private _retained: AedesPacket[];
  private _subscriptions: Map<string, AedesPersistenceSubscription>;
  private _clientsCount: number;
  private _trie: any;
  private _outgoing: Record<string, AedesPacket[]>;
  private _incoming: Incoming;
  private _wills: Record<string, WillPacket>;

  constructor();

  storeRetained: (
    packet: AedesPacket,
    cb: (error: Error | null) => void
  ) => void;

  createRetainedStream: (pattern: string) => Readable;

  createRetainedStreamCombi: (patterns: string[]) => Readable;

  addSubscriptions: (
    client: Client,
    subs: AedesPersistenceSubscription[],
    cb: (error: Error | null, client: Client) => void
  ) => void;

  removeSubscriptions: (
    client: Client,
    subs: AedesPersistenceSubscription[],
    cb: (error: Error | null, client: Client) => void
  ) => void;

  subscriptionsByClient: (
    client: Client,
    cb: (
      error: Error | null,
      subs: AedesPersistenceSubscription[],
      client: Client
    ) => void
  ) => void;

  countOffline: (
    cb: (
      error: Error | null,
      subscriptionsCount: number,
      clientsCount: number
    ) => void
  ) => void;

  subscriptionsByTopic: (
    pattern: string,
    cb: (error: Error | null, subs: AedesPersistenceSubscription[]) => void
  ) => void;

  cleanSubscriptions: (
    client: Client,
    cb: (error: Error | null, client: Client) => void
  ) => void;

  outgoingEnqueue: (
    sub: AedesPersistenceSubscription,
    packet: AedesPacket,
    cb: (error: Error | null) => void
  ) => void;

  outgoingEnqueueCombi: (
    subs: AedesPersistenceSubscription[],
    packet: AedesPacket,
    cb: (error: Error | null) => void
  ) => void;

  outgoingUpdate: (
    client: Client,
    packet: AedesPacket,
    cb: (error: Error | null, client: Client, packet: AedesPacket) => void
  ) => void;

  outgoingClearMessageId: (
    client: Client,
    packet: AedesPacket,
    cb: (error?: Error | null, packet?: AedesPacket) => void
  ) => void;

  outgoingStream: (client: Client) => Readable;

  incomingStorePacket: (
    client: Client,
    packet: AedesPacket,
    cb: (error: Error | null) => void
  ) => void;

  incomingGetPacket: (
    client: Client,
    packet: AedesPacket,
    cb: (error: Error | null, packet: AedesPacket) => void
  ) => void;

  incomingDelPacket: (
    client: Client,
    packet: AedesPacket,
    cb: (error: Error | null) => void
  ) => void;

  putWill: (
    client: Client,
    packet: AedesPacket,
    cb: (error: Error | null, client: Client) => void
  ) => void;

  getWill: (
    client: Client,
    cb: (error: Error | null, will: any, client: Client) => void
  ) => void;

  delWill: (
    client: Client,
    cb: (error: Error | null, will: any, client: Client) => void
  ) => void;

  streamWill: (brokers: Brokers) => Readable;

  getClientList: (topic: string) => Readable;

  destroy: (cb?: () => void) => void;
}

export default function aedesMemoryPersistence(): AedesMemoryPersistence;
