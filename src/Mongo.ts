import mongodb from 'mongodb';

import { merge } from '@amaui/utils';
import { Query } from '@amaui/models';
import { ConnectionError } from '@amaui/errors';
import AmauiLog from '@amaui/log';
import AmauiSubscription from '@amaui/subscription';

export interface IMongoOptions {
  name?: string;
  uri?: string;
}

export interface IDefaults {
  aggregateOptions?: mongodb.AggregateOptions;
  limitCount?: number;
}

export const mongoOptionsDefault: IMongoOptions = {};

export class Mongo {
  public db: mongodb.Db;
  public connected = false;
  public client: mongodb.MongoClient;
  private amalog: AmauiLog;
  private options_: IMongoOptions = mongoOptionsDefault;
  public collections: Array<mongodb.CollectionInfo>;
  // For listening on mongo events
  public subscription = new AmauiSubscription();
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

    this.amalog = new AmauiLog({
      arguments: {
        pre: ['Mongo'],
      },
    });
  }

  public get connection(): Promise<mongodb.Db> | Error {
    return new Promise(async resolve => {
      if (this.connected) return resolve(this.db);

      try {
        return resolve(await this.connect());
      }
      catch (error) {
        throw error;
      }
    });
  }

  public get disconnect(): Promise<void> {
    return new Promise(async resolve => {
      if (this.client && this.client.close) {
        await this.client.close();

        this.amalog.important(`Disconnected`);

        this.connected = false;
        this.db = undefined;
        this.client = undefined;

        this.subscription.emit('disconnected');

        return resolve();
      }

      resolve();
    });
  }

  public getCollections(refetch = false): Promise<Array<mongodb.CollectionInfo>> {
    return new Promise(async resolve => {
      try {
        if (this.collections && !refetch) return resolve(this.collections);

        this.collections = await this.db.listCollections().toArray();

        return resolve(this.collections);
      }
      catch (error) {
        throw error;
      }
    });
  }

  // Be very careful with this one,
  // it drops the entire database,
  // usually used for testing only
  public async reset(name: string): Promise<void> {
    if (this.db && name && this.db.databaseName === name) {
      await this.db.dropDatabase();

      this.amalog.important(`Reset`);

      this.subscription.emit('reset');
    }
  }

  private connect(): Promise<mongodb.Db | undefined> {
    return new Promise(async resolve => {
      const { uri, name } = this.options;

      try {
        this.client = await mongodb.MongoClient.connect(uri);

        this.db = this.client.db(name);
        this.connected = true;

        this.amalog.info(`Connected`);

        this.client.on('close', (event: any) => {
          this.amalog.warn(`Connection closed`, event);

          this.subscription.emit('disconnected');

          this.connected = false;

          setTimeout(() => {
            if (!this.connected) throw new ConnectionError(`Reconnect failed`);
          }, 1e4);
        });

        // Get meta about existing collections
        const collections = await this.getCollections(true);

        // Add collections to Query model
        Query.collections = collections.map(collection => collection.name);

        this.subscription.emit('connected');

        return resolve(this.db);
      }
      catch (error) {
        this.amalog.warn(`Connection error`, error);

        this.subscription.emit('error', error);

        throw new ConnectionError(error);
      }
    });
  }

}

export default Mongo;
