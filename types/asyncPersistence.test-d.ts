import type { Brokers, Client, Subscription } from 'aedes';
import type { AedesPacket } from 'aedes-packet';
import type { QoS } from 'mqtt-packet';
import type { Readable } from 'node:stream';
import { expectType } from 'tsd';
import MemoryPersistence from './asyncPersistence';
import type { AedesPersistenceSubscription, WillPacket } from '.';

// Test constructor
const persistence = new MemoryPersistence();

// Test setup
expectType<Promise<void>>(persistence.setup());

// Test storeRetained
expectType<Promise<void>>(
  persistence.storeRetained({
    brokerId: '',
    brokerCounter: 1,
    cmd: 'publish',
    qos: 0,
    dup: false,
    retain: false,
    topic: 'test',
    payload: 'test',
  })
);

// Test createRetainedStream
expectType<Readable>(persistence.createRetainedStream('pattern'));

// Test createRetainedStreamCombi
expectType<Readable>(persistence.createRetainedStreamCombi(['pattern1', 'pattern2']));

// Test addSubscriptions
expectType<Promise<void>>(
  persistence.addSubscriptions({} as Client, [] as Subscription[])
);

// Test removeSubscriptions
expectType<Promise<void>>(
  persistence.removeSubscriptions({} as Client, ['topic1', 'topic2'])
);

// Test subscriptionsByClient
expectType<Promise<{ topic: string; qos: QoS; rh?: number; rap?: number; nl?: number }[]>>(
  persistence.subscriptionsByClient({} as Client)
);

// Test countOffline
expectType<Promise<{ subsCount: number; clientsCount: number }>>(
  persistence.countOffline()
);

// Test subscriptionsByTopic
expectType<Promise<AedesPersistenceSubscription[]>>(
  persistence.subscriptionsByTopic('pattern')
);

// Test cleanSubscriptions
expectType<Promise<void>>(
  persistence.cleanSubscriptions({} as Client)
);

// Test outgoingEnqueue
expectType<Promise<void>>(
  persistence.outgoingEnqueue({ clientId: '' }, {} as AedesPacket)
);

// Test outgoingEnqueueCombi
expectType<Promise<void>>(
  persistence.outgoingEnqueueCombi([{ clientId: '' }], {} as AedesPacket)
);

// Test outgoingUpdate
expectType<Promise<void>>(
  persistence.outgoingUpdate({} as Client, {} as AedesPacket)
);

// Test outgoingClearMessageId
expectType<Promise<AedesPacket>>(
  persistence.outgoingClearMessageId({} as Client, {} as AedesPacket)
);

// Test outgoingStream
expectType<Readable>(persistence.outgoingStream({} as Client));

// Test incomingStorePacket
expectType<Promise<void>>(
  persistence.incomingStorePacket({} as Client, {} as AedesPacket)
);

// Test incomingGetPacket
expectType<Promise<AedesPacket>>(
  persistence.incomingGetPacket({} as Client, {} as AedesPacket)
);

// Test incomingDelPacket
expectType<Promise<void>>(
  persistence.incomingDelPacket({} as Client, {} as AedesPacket)
);

// Test putWill
expectType<Promise<void>>(
  persistence.putWill({} as Client, {} as AedesPacket)
);

// Test getWill
expectType<Promise<WillPacket | undefined>>(
  persistence.getWill({} as Client)
);

// Test delWill
expectType<Promise<WillPacket | undefined>>(
  persistence.delWill({} as Client)
);

// Test streamWill
expectType<Readable>(persistence.streamWill({} as Brokers));
expectType<Readable>(persistence.streamWill());

// Test getClientList
expectType<Readable>(persistence.getClientList('topic'));

// Test destroy
expectType<Promise<void>>(persistence.destroy());