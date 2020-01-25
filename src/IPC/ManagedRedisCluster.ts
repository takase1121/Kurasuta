//@ts-ignore
import fastRedis = require('redis-fast-driver');
//@ts-ignore
import a85 = require('a85-native');
import { EventEmitter } from 'events';
import { deepClone } from '../Util/Util';

/**
 * Provides a managed pool with pubsub support
 */
export class ManagedRedisCluster extends EventEmitter {
    /** Minimum amount of clients the pool should maintain */
    private minPool: number;
    /** Maximum amount of clients the pool makes before throwing errors */
    private maxPool: number;
    /** Time to live of each client */
    private poolTTL: number;
    /** Client options */
    private poolOptions: object;

    /** The pool */
    private pool = new Map<string, fastRedis>();
    /** The id of pools that are currently in use */
    private poolUsed = new Set<string>();
    /** Pool used for pub/sub. Normally it should be only 1 */
    private poolUsedPS: string = '';
    /** The topics that the client is subscribed to */
    private PSTopics: string[] = [];
    
    /**
     * Make a new instance
     * @param poolOptions
     * @param [poolOptions.minPool=1] Minimum amount of clients to maintain
     * @param [poolOptions.maxPool=10] Maximum amount of clients to maintain
     * @param [poolOptions.ttl=600000] Time to live for each clients before being evicted
     * @param redisOptions Options passed to Redis client
     */
    constructor(
        { minPool = 1, maxPool = 10, ttl = 600000}: { minPool: number, maxPool: number, ttl: number },
        redisOptions: object
    ) {
        super();

        this.minPool = minPool;
        this.maxPool = maxPool;
        this.poolTTL = ttl;
        this.poolOptions = deepClone(redisOptions);
        
        for (let i = 0; i < this.minPool; i++) {
            this.create(this.poolOptions);
        }
    }

    /**
     * Executes a command
     * @param command The command. `Buffer` will be converted into ascii85 strings and prefixed with `\u0002` and sent to Redis
     */
    run(command: (string|Buffer)[]): Promise<any> {
        const client = this.acquire();
        client.lastUsedTime = Date.now();
        return client.rawCallAsync(command.map((x: string|Buffer) => Buffer.isBuffer(x) ? '\u0002' + a85.stringify(x) : x))
            .then((x: any) => {
                this.release(client);
                return x;
            });
    }

    /**
     * Subscribe to topics
     * @param topic The topics to subscribe to. This uses PSUBSCRIBE, so mind your patterns.
     */
    subscribe(topic: string[]): Promise<boolean> {
        const client = this.acquire();
        client.lastUsedTime = Date.now();

        this.poolUsedPS = client.name;
        this.PSTopics = [...this.PSTopics, ...topic];
        
        client.rawCall(['PSUBSCRIBE', ...topic], this.onPubsubMessage.bind(this, client));
        
        return Promise.resolve(true);
    }

    /**
     * Unsubscribe from topics 
     * @param topic The topics to unsubscribe from. This uses PUNSUBSCRIBE, so mind your patterns.
     */
    unsubscribe(topic: string[]): Promise<boolean> {
        const client = this.acquire();
        client.lastUsedTime = Date.now();

        this.PSTopics = [...this.PSTopics].filter((x: string) => !topic.includes(x));
        if (this.PSTopics.length < 1) {
            // we can turn off the pubsub
            return client.rawCallAsync(['PUNSUBSCRIBE', ...topic])
                .then(() => this.poolUsedPS = '')
                .then(() => true);
        }
        
        return client.rawCallAsync(['PUNSUBSCRIBE', ...topic])
            .then(() => true);
    }

    /**
     * Creates a client
     * @param options Redis client
     */
    private create(options: object) {
        const uniqueID = `${process.pid}-${Date.now()}-${Math.floor(Math.random() * 4096)}`;
        const client = new fastRedis(options);
        client.name = uniqueID;
        client.lastUsedTime = Date.now();
        
        client.on('connect', this.onClientConnect.bind(this, client));
        client.on('ready', this.onClientReady.bind(this, client));
        client.on('disconnect', this.onClientDisconnect.bind(this, client));
        client.on('reconnecting', this.onClientReconnect.bind(this, client));
        client.once('error',this.onClientError.bind(this, client));

        this.pool.set(uniqueID, client);

        return uniqueID;
    }

    /**
     * Evicts unused clients
     */
    private evict() {
        const currentTime = Date.now();
        for (const client of this.pool.values()) {
            if (currentTime - client.lastUsedTime! >= this.poolTTL && this.pool.size > this.minPool && client.name !== this.poolUsedPS) {
                client.end();
                this.pool.delete(client.name);
            }
        }
    }

    /**
     * Get a client that is not in used. After using the client it must be put back to the pool via `ManagedRedisCluster#release()`
     * @param id 
     */
    private acquire(id?: string) {
        if (id) {
            this.poolUsed.add(id);
            return this.pool.get(id);
        }

        // get a client by comparing their queue sizes
        const clientList = [...this.pool.values()]
            .filter((x: any) => !this.poolUsed.has(x) && x !== this.poolUsedPS)
            .sort((a: any, b: any) => a.queue.length - b.queue.length);

        if (clientList.length === 0) {
            if (this.pool.size < this.maxPool) {
                // expand the pool
                const id = this.create(this.poolOptions);
                this.poolUsed.add(id);
                return this.pool.get(id);
            } else {
                // cannot expand, we throw error
                throw new Error('All clients are buzy, come back later');
            }
        }

        const leastBusyClient = clientList.shift();
        this.poolUsed.add(leastBusyClient.id);
        return leastBusyClient;
    }

    /**
     * Put the client back in the pool
     * @param client Redis client
     */
    private release(client: fastRedis) {
        this.poolUsed.delete(client.name);
    }


    private onClientConnect(client: fastRedis) {
        this.emit('connect', client.name);
    }

    private onClientReady(client: fastRedis) {
        this.emit('ready', client.name);
    }

    private onClientDisconnect(client: fastRedis) {
        this.emit('disconnect', client.name);
    }

    private onClientReconnect(client: fastRedis, n: number) {
        this.emit('reconnecting', client.name, n);
    }

    private onClientError(client: fastRedis, error: Error) {
        this.emit('error', client.name, error);
        client.removeAllListeners(); // its really fine to do this, because i find no interal listeners being added to it

        // this client is now spoiled; we remove it from the list
        this.pool.delete(client.name);

        // if the pool is empty or below min pool size, generate it back
        if (this.pool.size < this.minPool) {
            for (let i = 0; i < this.minPool; i++) {
                this.create(this.poolOptions);
            }
        }
    }

    private onPubsubMessage(client: fastRedis, message: any) {
        const topic = message[2];
        const rawData = message.pop();
        const data = rawData.startsWith('\u0002') ? a85.parse(rawData.slice(1)) : rawData;
        this.emit('message', {
            client,
            topic,
            data
        });
    }
}