import type { Client } from 'aedes';
import type { AedesPacket } from 'aedes-packet';
import type { Readable } from 'stream';

export type { AedesPacket as Packet } from 'aedes-packet';

export interface AedesPersistenceSubscription {
  clientId?: string;
  topic: string;
  qos: number;
}

type LastHearthbeatTimestamp = Date;

export interface Brokers {
  [brokerId: string]: LastHearthbeatTimestamp;
}

export type CallbackError = Error | null | undefined;

export type WillPacket = AedesPacket & { [key: string]: any };

interface Incoming {
  [clientId: string]: { [messageId: string]: AedesPacket };
}

export class AedesMemoryPersistence {
  private _retained: AedesPacket[];
  private _subscriptions: Map<string, Map<string, number>>;
  private _clientsCount: number;
  private _trie: any;
  private _outgoing: Record<string, AedesPacket[]>;
  private _incoming: Incoming;
  private _wills: Record<string, WillPacket>;

  constructor();

  storeRetained: (
    packet: AedesPacket,
    cb: (error: CallbackError) => void
  ) => void;

  createRetainedStream: (pattern: string) => Readable;

  createRetainedStreamCombi: (patterns: string[]) => Readable;

  addSubscriptions: (
    client: Client,
    subs: AedesPersistenceSubscription[],
    cb: (error: CallbackError, client: Client) => void
  ) => void;

  removeSubscriptions: (
    client: Client,
    subs: AedesPersistenceSubscription[],
    cb: (error: CallbackError, client: Client) => void
  ) => void;

  subscriptionsByClient: (
    client: Client,
    cb: (
      error: CallbackError,
      subs: AedesPersistenceSubscription[],
      client: Client
    ) => void
  ) => void;

  countOffline: (
    cb: (
      error: CallbackError,
      subscriptionsCount: number,
      clientsCount: number
    ) => void
  ) => void;

  subscriptionsByTopic: (
    pattern: string,
    cb: (error: CallbackError, subs: AedesPersistenceSubscription[]) => void
  ) => void;

  cleanSubscriptions: (
    client: Client,
    cb: (error: CallbackError, client: Client) => void
  ) => void;

  outgoingEnqueue: (
    sub: AedesPersistenceSubscription,
    packet: AedesPacket,
    cb: (error: CallbackError) => void
  ) => void;

  outgoingEnqueueCombi: (
    subs: AedesPersistenceSubscription[],
    packet: AedesPacket,
    cb: (error: CallbackError) => void
  ) => void;

  outgoingUpdate: (
    client: Client,
    packet: AedesPacket,
    cb: (error: CallbackError, client: Client, packet: AedesPacket) => void
  ) => void;

  outgoingClearMessageId: (
    client: Client,
    packet: AedesPacket,
    cb: (error?: CallbackError, packet?: AedesPacket) => void
  ) => void;

  outgoingStream: (client: Client) => Readable;

  incomingStorePacket: (
    client: Client,
    packet: AedesPacket,
    cb: (error: CallbackError) => void
  ) => void;

  incomingGetPacket: (
    client: Client,
    packet: AedesPacket,
    cb: (error: CallbackError, packet: AedesPacket) => void
  ) => void;

  incomingDelPacket: (
    client: Client,
    packet: AedesPacket,
    cb: (error: CallbackError) => void
  ) => void;

  putWill: (
    client: Client,
    packet: AedesPacket,
    cb: (error: CallbackError, client: Client) => void
  ) => void;

  getWill: (
    client: Client,
    cb: (error: CallbackError, will: WillPacket, client: Client) => void
  ) => void;

  delWill: (
    client: Client,
    cb: (error: CallbackError, will: WillPacket, client: Client) => void
  ) => void;

  streamWill: (brokers: Brokers) => Readable;

  getClientList: (topic: string) => Readable;

  destroy: (cb?: (error: CallbackError) => void) => void;
}

export default function aedesMemoryPersistence(): AedesMemoryPersistence;
