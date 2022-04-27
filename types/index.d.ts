import type { Brokers, Client, Subscription } from "aedes";
import type { AedesPacket } from "aedes-packet";
import type { Readable } from "stream";

export type { AedesPacket as Packet } from "aedes-packet";

type ClientId = Subscription["clientId"];
type MessageId = AedesPacket["messageId"];
type Topic = Subscription["topic"];
type TopicPattern = Subscription["topic"];
type QoS = Subscription["qos"];

type Incoming = {
  // tsc accepts:
  // [messageId: MessageId]: AedesPacket;
  // Workaround for tsd 0.20:
  [messageId: number]: AedesPacket;
};

export interface AedesPersistenceSubscription {
  clientId: ClientId;
  topic: Topic;
  qos?: QoS;
}

export type CallbackError = Error | null | undefined;

export type WillPacket = AedesPacket & { [key: string]: any };

export interface AedesPersistence {
  storeRetained: (
    packet: AedesPacket,
    cb: (error: CallbackError) => void,
  ) => void;

  createRetainedStream: (pattern: TopicPattern) => Readable;

  createRetainedStreamCombi: (patterns: TopicPattern[]) => Readable;

  addSubscriptions: (
    client: Client,
    subs: Subscription[],
    cb: (error: CallbackError, client: Client) => void,
  ) => void;

  removeSubscriptions: (
    client: Client,
    subs: Subscription[],
    cb: (error: CallbackError, client: Client) => void,
  ) => void;

  subscriptionsByClient: (
    client: Client,
    cb: (
      error: CallbackError,
      subs: { topic: Topic; qos: QoS }[],
      client: Client,
    ) => void,
  ) => void;

  countOffline: (
    cb: (
      error: CallbackError,
      subscriptionsCount: number,
      clientsCount: number,
    ) => void,
  ) => void;

  subscriptionsByTopic: (
    pattern: TopicPattern,
    cb: (error: CallbackError, subs: AedesPersistenceSubscription[]) => void,
  ) => void;

  cleanSubscriptions: (
    client: Client,
    cb: (error: CallbackError, client: Client) => void,
  ) => void;

  outgoingEnqueue: (
    sub: { clientId: ClientId },
    packet: AedesPacket,
    cb: (error: CallbackError) => void,
  ) => void;

  outgoingEnqueueCombi: (
    subs: { clientId: ClientId }[],
    packet: AedesPacket,
    cb: (error: CallbackError) => void,
  ) => void;

  outgoingUpdate: (
    client: Client,
    packet: AedesPacket,
    cb: (error: CallbackError, client: Client, packet: AedesPacket) => void,
  ) => void;

  outgoingClearMessageId: (
    client: Client,
    packet: AedesPacket,
    cb: (error?: CallbackError, packet?: AedesPacket) => void,
  ) => void;

  outgoingStream: (client: Client) => Readable;

  incomingStorePacket: (
    client: Client,
    packet: AedesPacket,
    cb: (error: CallbackError) => void,
  ) => void;

  incomingGetPacket: (
    client: Client,
    packet: AedesPacket,
    cb: (error: CallbackError, packet: AedesPacket) => void,
  ) => void;

  incomingDelPacket: (
    client: Client,
    packet: AedesPacket,
    cb: (error: CallbackError) => void,
  ) => void;

  putWill: (
    client: Client,
    packet: AedesPacket,
    cb: (error: CallbackError, client: Client) => void,
  ) => void;

  getWill: (
    client: Client,
    cb: (error: CallbackError, will: WillPacket, client: Client) => void,
  ) => void;

  delWill: (
    client: Client,
    cb: (error: CallbackError, will: WillPacket, client: Client) => void,
  ) => void;

  streamWill: (brokers: Brokers) => Readable;

  getClientList: (topic: string) => Readable;

  destroy: (cb?: (error: CallbackError) => void) => void;
}

export class AedesMemoryPersistence implements AedesPersistence {
  #retained: Map<Topic, AedesPacket>;
  #subscriptions: Map<
    ClientId,
    Map<
      Topic,
      QoS
    >
  >;
  #clientsCount: number;
  #trie: any;
  #outgoing: Map<ClientId, AedesPacket[]>;
  #incoming: Map<ClientId, Incoming>;
  #wills: Map<ClientId, WillPacket>;

  constructor();

  storeRetained: (
    packet: AedesPacket,
    cb: (error: CallbackError) => void,
  ) => void;

  createRetainedStream: (pattern: TopicPattern) => Readable;

  createRetainedStreamCombi: (patterns: TopicPattern[]) => Readable;

  addSubscriptions: (
    client: Client,
    subs: Subscription[],
    cb: (error: CallbackError, client: Client) => void,
  ) => void;

  removeSubscriptions: (
    client: Client,
    subs: Subscription[],
    cb: (error: CallbackError, client: Client) => void,
  ) => void;

  subscriptionsByClient: (
    client: Client,
    cb: (
      error: CallbackError,
      subs: { topic: string; qos: QoS }[],
      client: Client,
    ) => void,
  ) => void;

  countOffline: (
    cb: (
      error: CallbackError,
      subscriptionsCount: number,
      clientsCount: number,
    ) => void,
  ) => void;

  subscriptionsByTopic: (
    pattern: string,
    cb: (error: CallbackError, subs: AedesPersistenceSubscription[]) => void,
  ) => void;

  cleanSubscriptions: (
    client: Client,
    cb: (error: CallbackError, client: Client) => void,
  ) => void;

  #outgoingEnqueuePerSub: (
    sub: { clientId: ClientId },
    packet: AedesPacket,
  ) => void;

  outgoingEnqueue: (
    sub: { clientId: ClientId },
    packet: AedesPacket,
    cb: (error: CallbackError) => void,
  ) => void;

  outgoingEnqueueCombi: (
    sub: { clientId: ClientId }[],
    packet: AedesPacket,
    cb: (error: CallbackError) => void,
  ) => void;

  outgoingUpdate: (
    client: Client,
    packet: AedesPacket,
    cb: (error: CallbackError, client: Client, packet: AedesPacket) => void,
  ) => void;

  outgoingClearMessageId: (
    client: Client,
    packet: AedesPacket,
    cb: (error?: CallbackError, packet?: AedesPacket) => void,
  ) => void;

  outgoingStream: (client: Client) => Readable;

  incomingStorePacket: (
    client: Client,
    packet: AedesPacket,
    cb: (error: CallbackError) => void,
  ) => void;

  incomingGetPacket: (
    client: Client,
    packet: AedesPacket,
    cb: (error: CallbackError, packet: AedesPacket) => void,
  ) => void;

  incomingDelPacket: (
    client: Client,
    packet: AedesPacket,
    cb: (error: CallbackError) => void,
  ) => void;

  putWill: (
    client: Client,
    packet: AedesPacket,
    cb: (error: CallbackError, client: Client) => void,
  ) => void;

  getWill: (
    client: Client,
    cb: (error: CallbackError, will: WillPacket, client: Client) => void,
  ) => void;

  delWill: (
    client: Client,
    cb: (error: CallbackError, will: WillPacket, client: Client) => void,
  ) => void;

  streamWill: (brokers: Brokers) => Readable;

  getClientList: (topic: string) => Readable;

  destroy: (cb?: (error: CallbackError) => void) => void;
}

export default function aedesMemoryPersistence(): AedesMemoryPersistence;
