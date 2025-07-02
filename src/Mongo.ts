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
  public indexed = false;
  public retrying = false;

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
          for (const index of item.indexes) await this.db.collection(name).createIndex(index.keys, index.options);
        }
      }
    }

    return true;
  }

  public get connection(): Promise<mongodb.Db> | Error {
    return new Promise(async resolve => {
      if (this.connected) return resolve(this.db);

      let db = null;

      this.retrying = true;

      while (!db) {
        try {
          db = await this.connect();

          // Create indexes
          if (!this.indexed) {
            await this.createIndexes();

            this.indexed = true;
          }

          this.retrying = false;

          return resolve(db);
        }
        catch (error) {
          this.onesyLog.important('get connection() error', error);

          await wait(1e3);
        }
      }
    });
  }

  public get disconnect(): Promise<void> {
    this.connected = false;
    this.db = undefined;
    this.client = undefined;

    return new Promise(async resolve => {
      try {
        if (this.client && this.client.close) {
          await this.client.close();

          this.onesyLog.important(`Disconnected`);

          this.subscription.emit('disconnected');
        }
      }
      catch (error) {
        this.onesyLog.important('get disconnect error', error);
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

  private async connect(): Promise<mongodb.Db | undefined> {
    const { uri, name } = this.options;

    try {
      const clientOptions: mongodb.MongoClientOptions = {
        connectTimeoutMS: 10000,
        socketTimeoutMS: 15000,
        retryWrites: true,
        retryReads: true,
        serverSelectionTimeoutMS: 5000
      };

      this.client = await mongodb.MongoClient.connect(uri, clientOptions);

      this.db = this.client.db(name);

      this.connected = true;

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

      return null;
    }
  }

  private setupConnectionListeners(): void {
    if (!this.client) return;

    this.client.on('close', () => {
      this.disconnect;
    });

    this.client.on('error', error => {
      this.onesyLog.warn('MongoDB connection error', error);

      if (!this.retrying) this.connection;
    });

    this.client.on('reconnect', () => {
      this.onesyLog.info('MongoDB reconnected');

      this.connected = true;

      this.subscription.emit('reconnected');
    });
  }

}

export default Mongo;
