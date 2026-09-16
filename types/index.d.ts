import type { Brokers, Client, Subscription } from "aedes";
import type { AedesPacket } from "aedes-packet";
import type { Readable } from "stream";

export type { AedesPacket as Packet } from "aedes-packet";

type ClientId = Client["id"];
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
      // `subscriptionIdentifier` is the MQTT 5.0 Subscription Identifier, present
      // only when the subscription carried one; aedes restores it from here on a
      // non-clean reconnect.
      subs: { topic: Topic; qos: QoS; subscriptionIdentifier?: number }[],
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

  /**
   * Removes every stored incoming (QoS 2) packet for the given client.
   * Called by aedes when a session is discarded (CONNECT with cleanSession),
   * see [MQTT-3.1.2-6].
   *
   * Required of a persistence since v11.0.0, and declared non-optional here.
   * aedes itself feature-detects the method and skips it when absent, so an
   * older persistence keeps working rather than crashing — but it stays exposed
   * to cross-session QoS 2 dedup residue surviving a clean-session reconnect.
   */
  cleanIncoming: (
    client: Client,
    cb: (error: CallbackError, client: Client) => void,
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

  cleanIncoming: (
    client: Client,
    cb: (error: CallbackError, client: Client) => void,
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
