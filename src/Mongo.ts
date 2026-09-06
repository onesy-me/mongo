import * as mongodb from 'mongodb';

import { merge, wait } from '@onesy/utils';
import { Query } from '@onesy/models';
import OnesyLog from '@onesy/log';
import { IOnesyLogOptions } from '@onesy/log/OnesyLog';
import OnesySubscription from '@onesy/subscription';

export interface IMongoCollectionIndex {
  name: string;

  indexes: Array<{
    keys: mongodb.IndexSpecification;
    options?: mongodb.CreateIndexesOptions;
  }>;
}

export interface IMongoOptions {
  name?: string;
  uri?: string;

  log_options?: IOnesyLogOptions;

  indexes?: IMongoCollectionIndex[];

  reconnectInterval?: number;
  maxReconnectAttempts?: number;

  // New connection pool options
  maxPoolSize?: number;
  minPoolSize?: number;
  maxIdleTimeMS?: number;
  appName?: string;
}

export interface IDefaults {
  aggregateOptions?: mongodb.AggregateOptions;
  limitCount?: number;
}

export const mongoOptionsDefault: IMongoOptions = {
  reconnectInterval: 5000,
  maxReconnectAttempts: 10,
  maxPoolSize: 20,
  minPoolSize: 5,
  maxIdleTimeMS: 6e7,
  appName: 'api'
};

export class Mongo {
  public db: mongodb.Db;
  public connected = false;
  public client: mongodb.MongoClient;
  public onesyLog: OnesyLog;
  private options_: IMongoOptions = mongoOptionsDefault;
  public collections: Array<mongodb.CollectionInfo>;
  // For listening on mongo events
  public subscription = new OnesySubscription();
  public indexed = false;
  public retrying = false;
  public isReconnecting = false;
  public reconnectAttempts = 0;
  private static mongos = {};

  public static defaults: IDefaults = {
    aggregateOptions: { allowDiskUse: false },
    limitCount: 1e3
  };

  public get options(): IMongoOptions {
    return this.options_;
  }

  public set options(options: IMongoOptions) {
    this.options_ = merge(options, mongoOptionsDefault);
  }

  public get local() {
    if (!Mongo.mongos[this.options.uri]) Mongo.mongos[this.options.uri] = {};

    return Mongo.mongos[this.options.uri];
  }

  public constructor(options: IMongoOptions = mongoOptionsDefault) {
    this.options = options;

    this.onesyLog = new OnesyLog({
      arguments: {
        pre: ['Mongo']
      },

      ...options.log_options
    });
  }

  public async createIndexes(): Promise<any> {
    if (this.options.indexes?.length) {
      for (const item of this.options.indexes) {
        const name = item.name;

        if (name && item.indexes?.length) {
          for (const index of item.indexes) await this.db.collection(name).createIndex(index.keys, index.options);
        }
      }
    }

    return true;
  }

  public get connection(): Promise<mongodb.Db> | Error {
    if (this.connected && this.db) {
      return Promise.resolve(this.db);
    }

    if (this.local.connectionPromise) {
      return this.local.connectionPromise;
    }

    console.log(`(CONNECTION PROMISE) ${process.env.NODE_APP_INSTANCE}`);

    this.local.connectionPromise = new Promise(async (resolve, reject) => {
      try {
        this.onesyLog.debug('🟡 (connection, no cache)', this.connected);

        let db = null;

        this.retrying = true;

        while (!db) {
          try {
            db = await this.connect();

            this.onesyLog.debug('🟡 (while (!db))', this.connected, db);

            // Create indexes
            if (!this.indexed) {
              await this.createIndexes();

              this.indexed = true;
            }

            this.retrying = false;
            this.local.connectionPromise = null;

            resolve(db);
          }
          catch (error) {
            this.onesyLog.important('get connection() error', error);

            await wait(1e3);
          }
        }
      } catch (error) {
        this.local.connectionPromise = null;
        reject(error);
      }
    });

    return this.local.connectionPromise;
  }

  public async disconnect(): Promise<void> {
    if (!this.client) return;

    try {
      this.onesyLog.debug('🟡 (disconnect) connected = false');

      this.connected = false;
      this.isReconnecting = false;
      this.reconnectAttempts = 0;

      await this.client.close();

      this.db = undefined;
      this.client = undefined;

      this.onesyLog.important('Disconnected');
      this.subscription.emit('disconnected');
    }
    catch (error) {
      this.onesyLog.important('Disconnect error', error);
    }
  }

  public async getCollections(refetch = false): Promise<Array<mongodb.CollectionInfo>> {
    if (this.collections && !refetch) return this.collections;

    try {
      this.collections = await this.db.listCollections().toArray();

      return this.collections;
    }
    catch (error) {
      this.onesyLog.important('getCollections error', error);

      throw error;
    }
  }

  // Be very careful with this one,
  // it drops the entire database,
  // usually used for testing only
  public async reset(name: string): Promise<void> {
    if (this.db && name && this.db.databaseName === name) {
      await this.db.dropDatabase();

      this.onesyLog.important(`Reset`);

      this.subscription.emit('reset');
    }
  }

  public async health(): Promise<{ connected: boolean; poolStats?: any }> {
    if (!this.connected || !this.client) {
      return { connected: false };
    }

    try {
      // Ping the database to check connection
      await this.db.command({ ping: 1 });

      return {
        connected: true,
        poolStats: {
          // You can expose pool stats here if needed
        }
      };
    } catch (error) {
      this.onesyLog.debug('🟡 (health, error) connected = false');

      this.connected = false;

      return { connected: false };
    }
  }

  public async connect(): Promise<mongodb.Db | undefined> {
    if (this.connected && this.db) {
      return this.db;
    }

    if (this.isReconnecting) {
      return new Promise(resolve => {
        const checkConnection = () => {
          if (this.connected && this.db) {
            resolve(this.db);
          } else {
            setTimeout(checkConnection, 100);
          }
        };

        checkConnection();
      });
    }

    return this.connectWithRetry();
  }

  private async connectWithRetry(): Promise<mongodb.Db | undefined> {
    const { uri, name } = this.options;

    try {
      // Get pool size from options with fallbacks
      const maxPoolSize = this.options.maxPoolSize ?? 20;
      const minPoolSize = this.options.minPoolSize ?? 5;
      const maxIdleTimeMS = this.options.maxIdleTimeMS ?? 6e5;
      const appName = this.options.appName || 'api';

      const clientOptions: mongodb.MongoClientOptions = {
        connectTimeoutMS: 10000,
        socketTimeoutMS: 15000,
        retryWrites: true,
        retryReads: true,
        serverSelectionTimeoutMS: 5000,

        // Connection pool configuration
        maxPoolSize,
        minPoolSize,
        maxIdleTimeMS,

        // App identification
        appName: appName,

        // Compression for better performance
        compressors: ['zstd', 'zlib']
      };

      this.client = await mongodb.MongoClient.connect(uri, clientOptions);

      Mongo.mongos[uri] = {};

      this.db = this.client.db(name);

      this.onesyLog.debug('✅ (connectWithRetry) connected = true');

      this.connected = true;
      this.reconnectAttempts = 0;
      this.isReconnecting = false;

      console.log('📊📊📊 === CONNECTION ESTABLISHED client ===', this.client);

      console.log('📊📊📊 === CONNECTION ESTABLISHED db ===', this.db);

      this.onesyLog.info(`Connected to MongoDB (pool: ${minPoolSize}-${maxPoolSize}, idle: ${maxIdleTimeMS}ms, app: ${appName})`);

      this.onesyLog.info(`Connected to MongoDB (pool: ${minPoolSize}-${maxPoolSize}, app: ${appName})`);

      // Setup event listeners
      this.setupConnectionListeners();

      // Get meta about existing collections
      const collections = await this.getCollections(true);

      // Add collections to Query model
      Query.collections = collections.map(collection => collection.name);

      this.subscription.emit('connected');

      return this.db;
    }
    catch (error) {
      this.onesyLog.warn('Connection error', error);

      this.onesyLog.debug('🟡 (connectWithRetry, error) connected = false');

      this.connected = false;
      this.isReconnecting = true;

      // Exponential backoff for retries
      const baseDelay = this.options.reconnectInterval || 5000;
      const maxReconnectAttempts = this.options.maxReconnectAttempts || 10;
      const delay = Math.min(baseDelay * Math.pow(1.5, this.reconnectAttempts), 30000);

      this.reconnectAttempts++;

      if (this.reconnectAttempts <= maxReconnectAttempts) {
        this.onesyLog.info(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${maxReconnectAttempts})`);

        await wait(delay);

        return this.connectWithRetry();
      } else {
        this.onesyLog.error('Max reconnect attempts reached');

        this.isReconnecting = false;

        this.subscription.emit('error', new Error('Max reconnect attempts reached'));

        return null;
      }
    }
  }

  private setupConnectionListeners(): void {
    if (!this.client) return;

    this.client.on('close', () => {
      this.onesyLog.warn('MongoDB connection closed');

      this.onesyLog.debug('🟡 (setupConnectionListeners, close) connected = false');

      this.connected = false;
    });

    this.client.on('error', error => {
      this.onesyLog.warn('MongoDB connection error', error);

      this.onesyLog.debug('🟡 (setupConnectionListeners, error) connected = false');

      this.connected = false;

      // Only attempt reconnect if we're not already reconnecting
      if (!this.isReconnecting && this.retrying) {
        this.connect().catch(err => {
          this.onesyLog.error('Reconnect attempt failed', err);
        });
      }
    });

    this.client.on('reconnect', () => {
      this.onesyLog.info('MongoDB reconnected');

      this.onesyLog.debug('✅ (reconnect) connected = true');

      this.connected = true;
      this.isReconnecting = false;
      this.reconnectAttempts = 0;

      this.subscription.emit('reconnected');
    });

    // Debug
    // === POOL EVENTS ===
    this.client.on('connectionPoolCreated', (event) => {
      console.log('🟢 POOL CREATED at:', new Date().toISOString());
      console.log('  Max Size:', event);
    });

    this.client.on('connectionPoolReady', (event) => {
      console.log('✅ POOL READY at:', new Date().toISOString());
    });

    this.client.on('connectionPoolClosed', (event) => {
      console.log('🔴🔴🔴 POOL CLOSED at:', new Date().toISOString());
      console.log('🔴 Stack:', new Error().stack);
    });

    this.client.on('connectionPoolCleared', (event) => {
      console.log('🟡🟡🟡 POOL CLEARED at:', new Date().toISOString());
      console.log('🟡 All connections were removed!');
      console.log('🟡 Stack:', new Error().stack);
    });

    // === CONNECTION EVENTS ===
    this.client.on('connectionCreated', (event) => {
      console.log('🟢🟢🟢 CONNECTION CREATED at:', new Date().toISOString());
      console.log('  Connection ID:', event.connectionId);
    });

    this.client.on('connectionReady', (event) => {
      console.log('✅ CONNECTION READY at:', new Date().toISOString());
      console.log('  Connection ID:', event.connectionId);
    });

    this.client.on('connectionClosed', (event) => {
      console.log('🔴🔴🔴 CONNECTION CLOSED at:', new Date().toISOString());
      console.log('  Connection ID:', event.connectionId);
      console.log('  Reason:', event.reason || 'unknown');
    });

    // === CHECKOUT EVENTS ===
    this.client.on('connectionCheckOutStarted', (event) => {
      console.log('🟣 CHECK OUT STARTED at:', new Date().toISOString());
    });

    this.client.on('connectionCheckOutFailed', (event) => {
      console.log('❌ CHECK OUT FAILED at:', new Date().toISOString());
      console.log('  Reason:', event.reason);
    });

    this.client.on('connectionCheckedOut', (event) => {
      // You already have this
      console.log('🟢 Connection CHECKED OUT at:', new Date().toISOString());
    });

    this.client.on('connectionCheckedIn', (event) => {
      // You already have this
      console.log('🟣 Connection CHECKED IN at:', new Date().toISOString());
    });
  }

}

export default Mongo;
