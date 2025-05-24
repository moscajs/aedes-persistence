import type { Brokers, Client, Subscription } from "aedes";
import type { AedesPacket } from "aedes-packet";
import type { Readable } from "node:stream";
import type { AedesPersistenceSubscription, WillPacket } from "./index";

type ClientId = Client["id"];
type Topic = Subscription["topic"];
type TopicPattern = Subscription["topic"];
type QoS = Subscription["qos"];

interface SubscriptionData {
  qos: QoS;
  rh?: number;
  rap?: number;
  nl?: number;
}

declare class MemoryPersistence {
  #retained: Map<Topic, AedesPacket>;
  #subscriptions: Map<ClientId, Map<Topic, SubscriptionData>>;
  #outgoing: Map<ClientId, AedesPacket[]>;
  #incoming: Map<ClientId, { [messageId: number]: AedesPacket }>;
  #wills: Map<ClientId, WillPacket>;
  #clientsCount: number;
  #trie: any;

  constructor();

  setup(): Promise<void>;

  storeRetained(packet: AedesPacket): Promise<void>;

  createRetainedStream(pattern: TopicPattern): Readable;

  createRetainedStreamCombi(patterns: TopicPattern[]): Readable;

  addSubscriptions(client: Client, subs: Subscription[]): Promise<void>;

  removeSubscriptions(client: Client, subs: Topic[]): Promise<void>;

  subscriptionsByClient(client: Client): Promise<{ topic: Topic; qos: QoS; rh?: number; rap?: number; nl?: number }[]>;

  countOffline(): Promise<{ subsCount: number; clientsCount: number }>;

  subscriptionsByTopic(pattern: TopicPattern): Promise<AedesPersistenceSubscription[]>;

  cleanSubscriptions(client: Client): Promise<void>;

  #outgoingEnqueuePerSub(sub: { clientId: ClientId }, packet: AedesPacket): void;

  outgoingEnqueue(sub: { clientId: ClientId }, packet: AedesPacket): Promise<void>;

  outgoingEnqueueCombi(subs: { clientId: ClientId }[], packet: AedesPacket): Promise<void>;

  outgoingUpdate(client: Client, packet: AedesPacket): Promise<void>;

  outgoingClearMessageId(client: Client, packet: AedesPacket): Promise<AedesPacket>;

  outgoingStream(client: Client): Readable;

  incomingStorePacket(client: Client, packet: AedesPacket): Promise<void>;

  incomingGetPacket(client: Client, packet: AedesPacket): Promise<AedesPacket>;

  incomingDelPacket(client: Client, packet: AedesPacket): Promise<void>;

  putWill(client: Client, packet: AedesPacket): Promise<void>;

  getWill(client: Client): Promise<WillPacket | undefined>;

  delWill(client: Client): Promise<WillPacket | undefined>;

  streamWill(brokers?: Brokers): Readable;

  getClientList(topic: string): Readable;

  destroy(): Promise<void>;
}

export = MemoryPersistence;