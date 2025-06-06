import * as mongodb from 'mongodb';

import { merge } from '@onesy/utils';
import { Query } from '@onesy/models';
import { ConnectionError } from '@onesy/errors';
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
}

export interface IDefaults {
  aggregateOptions?: mongodb.AggregateOptions;
  limitCount?: number;
}

export const mongoOptionsDefault: IMongoOptions = {
  reconnectInterval: 5000,
  maxReconnectAttempts: 10
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
  public reconnectAttempts = 0;
  public reconnectTimeout: NodeJS.Timeout;
  public isExplicitDisconnect = false;

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
          for (const index of item.indexes) {
            await this.db.collection(name).createIndex(index.keys, index.options);
          }
        }
      }
    }

    return true;
  }

  public get connection(): Promise<mongodb.Db> | Error {
    return new Promise(async resolve => {
      if (this.connected) return resolve(this.db);

      try {
        const db = await this.connect();

        // Create indexes
        await this.createIndexes();

        return resolve(db);
      }
      catch (error) {
        throw error;
      }
    });
  }

  public get disconnect(): Promise<void> {
    this.isExplicitDisconnect = true;

    if (this.reconnectTimeout !== undefined) clearTimeout(this.reconnectTimeout);

    return new Promise(async resolve => {
      if (this.client && this.client.close) {
        await this.client.close();

        this.onesyLog.important(`Disconnected`);

        this.connected = false;
        this.db = undefined;
        this.client = undefined;

        this.subscription.emit('disconnected');

        return resolve();
      }

      resolve();
    });
  }

  public async getCollections(refetch = false): Promise<Array<mongodb.CollectionInfo>> {
    if (this.collections && !refetch) return this.collections;

    try {
      this.collections = await this.db.listCollections().toArray();

      return this.collections;
    }
    catch (error) {
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

  private async connect(): Promise<mongodb.Db | undefined> {
    const { uri, name } = this.options;

    this.isExplicitDisconnect = false;
    this.reconnectAttempts = 0;

    try {
      const clientOptions: mongodb.MongoClientOptions = {
        connectTimeoutMS: 10000,
        socketTimeoutMS: 45000,
        retryWrites: true,
        retryReads: true,
        serverSelectionTimeoutMS: 5000
      };

      this.client = await mongodb.MongoClient.connect(uri, clientOptions);

      this.db = this.client.db(name);

      this.connected = true;
      this.reconnectAttempts = 0;

      this.onesyLog.info('Connected to MongoDB');

      // event listeners
      this.setupConnectionListeners();

      // Get meta about existing collections
      const collections = await this.getCollections(true);

      // Add collections to Query model
      Query.collections = collections.map(collection => collection.name);

      this.subscription.emit('connected');

      return this.db;
    }
    catch (error) {
      this.onesyLog.warn('Initial connection error', error);

      this.subscription.emit('error', error);

      throw new ConnectionError(error);
    }
  }

  private setupConnectionListeners(): void {
    if (!this.client) return;

    this.client.on('close', () => {
      this.onDisconnect();
    });

    this.client.on('error', (err) => {
      this.onesyLog.warn('MongoDB connection error', err);

      this.onDisconnect();
    });

    this.client.on('reconnect', () => {
      this.onesyLog.info('MongoDB reconnected');

      this.connected = true;
      this.reconnectAttempts = 0;

      this.subscription.emit('reconnected');
    });
  }

  private onDisconnect(): void {
    if (this.isExplicitDisconnect) return;

    this.connected = false;

    this.onesyLog.warn('MongoDB connection lost');
    this.subscription.emit('disconnected');

    if (this.reconnectAttempts < this.options.maxReconnectAttempts) {
      this.reconnectAttempts++;

      const delay = this.options.reconnectInterval;

      this.onesyLog.info(`Attempting to reconnect (attempt ${this.reconnectAttempts}/${this.options.maxReconnectAttempts}) in ${delay / 1000} seconds`);

      // clear previous timeout
      if (this.reconnectTimeout !== undefined) clearTimeout(this.reconnectTimeout);

      this.reconnectTimeout = setTimeout(() => {
        this.reconnect().catch(error => {
          this.onesyLog.error('Reconnect attempt failed', error);
        });
      }, delay);
    } else {
      this.onesyLog.error(`Max reconnection attempts (${this.options.maxReconnectAttempts}) reached. Done.`);
    }
  }

  private async reconnect(): Promise<void> {
    try {
      await this.connect();
    }
    catch (error) {
      this.onDisconnect();
    }
  }

}

export default Mongo;
