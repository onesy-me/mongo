import * as mongodb from 'mongodb';
import express from 'express';

import is from '@amaui/utils/is';
import wait from '@amaui/utils/wait';
import setObjectValue from '@amaui/utils/setObjectValue';
import { TMethod, Query, IMongoResponse, getMongoMatch, IMongoSearchManyAdditional, IMongoSearchOneAdditional, MongoResponse, IClass, } from '@amaui/models';
import { AmauiMongoError, DeveloperError } from '@amaui/errors';
import AmauiDate from '@amaui/date/amaui-date';
import duration from '@amaui/date/duration';
import AmauiLog from '@amaui/log';

import Mongo from './Mongo';
import AmauiMongo from './AmauiMongo';

export interface IUpdateOrAddOptions extends mongodb.FindOneAndUpdateOptions {
  add_date?: boolean;
  update_date?: boolean;
}

export interface IUpdateOptions extends mongodb.FindOneAndUpdateOptions {
  update_date?: boolean;
}

export interface IUpdateManyOptions extends mongodb.UpdateOptions {
  update_date?: boolean;
}

export interface IAddOneOptions extends mongodb.InsertOneOptions {
  add_date?: boolean;
}

export interface IAddManyOptions extends mongodb.BulkWriteOptions {
  add_date?: boolean;
}

export class BaseCollection {
  private db_: mongodb.Db;
  protected collections: Record<string, mongodb.Collection> = {};
  protected amalog: AmauiLog;

  public constructor(
    protected collectionName: string,
    public mongo: Mongo,
    public Model?: IClass
  ) {
    if (!(mongo && mongo instanceof Mongo)) throw new AmauiMongoError(`Mongo instance is required`);
    if (!collectionName) throw new AmauiMongoError(`Collection name is required`);

    this.amalog = new AmauiLog({
      arguments: {
        pre: ['Mongo'],
      },
    });
  }

  public get sort(): Record<string, number> {
    return {
      [this.sortProperty]: this.sortAscending
    };
  }

  public get sortProperty(): string { return 'api_meta.added_at'; }

  public get sortAscending(): number { return -1; }

  public get addedProperty(): string { return 'api_meta.added_at'; }

  public get updatedProperty(): string { return 'api_meta.updated_at'; }

  public get projection(): object {

    return {
      _id: 1,
      meta: 1,
      data: 1,
      api_meta: 1
    };
  }

  public get db(): Promise<mongodb.Db> {
    return new Promise((async resolve => {
      if (!this.db_) this.db_ = await this.mongo.connection as mongodb.Db;

      return resolve(this.db_);
    }));
  }

  public async collection(
    name: string = this.collectionName,
    options: mongodb.CreateCollectionOptions = {}
  ): Promise<mongodb.Collection> {
    const db = await this.db;

    if (!this.collections[name]) this.collections[name] = db.collection(name);

    const collections = await this.mongo.getCollections();

    // Good to create a collection in advance if it doesn't exist atm
    // as it might fail if you wanna add a document within a transaction
    // on a non existing mongo collection atm
    if (!collections?.find(item => item.name === this.collectionName)) {
      const collection = await db.createCollection(this.collectionName, options);

      this.mongo.collections.push({ name: collection.collectionName });

      // Add collection to Query model collections
      Query.collections.push(collection.collectionName);
      Query.keys.allowed.push(collection.collectionName);

      this.amalog.info(`${this.collectionName} collection created`);
    }

    return this.collections[name];
  }

  public async transaction(method: TMethod, options = { retries: 5, retriesWait: 140 }): Promise<void | Error> {
    if (!is('function', method)) throw new DeveloperError('First argument has to be a function');

    const transactionOptions: mongodb.TransactionOptions = {
      readPreference: mongodb.ReadPreference.primary,
      readConcern: { level: 'local' },
      writeConcern: { w: 'majority' },
    };

    let response: any;

    const retriesTotal = is('number', options.retries) ? options.retries : 5;
    const retriesWait = is('number', options.retriesWait) ? options.retriesWait : 140;
    let retries = retriesTotal;

    let error: Error;
    let codeName = 'WriteConflict';

    while (
      codeName === 'WriteConflict' &&
      retries > 0
    ) {
      error = undefined;
      codeName = undefined;

      if (retries < retriesTotal) await wait(retriesWait);

      const session = this.mongo.client.startSession();

      try {
        response = await session.withTransaction(async () => await method(session), transactionOptions);
      }
      catch (error_) {
        error = error_;
        codeName = error_.codeName || error_.message?.codeName;
      }
      finally {
        await session.endSession();
      }

      retries--;
    }

    if (error) throw new DeveloperError(error);

    return response;
  }

  public async count(
    query: Query = new Query(),
    options: mongodb.CountDocumentsOptions = {}
  ): Promise<number> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const response = await collection.countDocuments(
        query?.queries.find[this.collectionName] || {},
        options
      );

      return this.response(start, collection, 'count', response);
    }
    catch (error) {
      this.response(start, collection, 'count');

      throw new AmauiMongoError(error);
    }
  }

  public async exists(
    query: Query,
    options: mongodb.FindOptions = {}
  ): Promise<boolean> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const response = await collection.findOne(
        query?.queries?.find[this.collectionName] || {},
        {
          projection: { _id: 1 },
          ...options
        }
      );

      return this.response(start, collection, 'exists', !!response);
    }
    catch (error) {
      this.response(start, collection, 'exists');

      throw new AmauiMongoError(error);
    }
  }

  public async find(
    query: Query,
    options: mongodb.FindOptions = {}
  ): Promise<IMongoResponse> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      if (!options.projection) options.projection = query.projection || this.projection;
      if (!options.sort) options.sort = (query.sort || this.sort as mongodb.Sort);
      if (!options.skip) options.skip = query.skip || 0;
      if (!options.limit) options.limit = query.limit || 15;

      const response_ = await collection.find(
        query.queries.find[this.collectionName],
        options
      ).toArray();

      const response = new MongoResponse(response_);

      response.sort = options.sort as any;
      response.size = response_.length;
      response.skip = options.skip;
      response.limit = options.limit;

      if (query.total) response['total'] = await collection.find(
        query.queries.find[this.collectionName],
        { projection: { _id: 1 } }
      ).count();

      return this.response(start, collection, 'find', response);
    }
    catch (error) {
      this.response(start, collection, 'find');

      throw new AmauiMongoError(error);
    }
  }

  public async findOne(
    query: Query,
    options: mongodb.FindOptions = {}
  ): Promise<any> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      if (!options.projection) options.projection = query.projection || this.projection;

      const response = collection.findOne(
        query?.queries.find[this.collectionName] || {},
        options
      );

      return this.response(start, collection, 'findOne', response);
    }
    catch (error) {
      this.response(start, collection, 'findOne');

      throw new AmauiMongoError(error);
    }
  }

  public async aggregate(
    query: Query = new Query(),
    options: mongodb.AggregateOptions = {}
  ): Promise<Array<mongodb.Document>> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const value = query?.queries?.aggregate?.[this.collectionName] || [];

      const response = collection.aggregate(
        value,
        {
          ...Mongo.defaults.aggregateOptions,
          ...options
        }
      ).toArray();

      return this.response(start, collection, 'aggregate', response);
    }
    catch (error) {
      this.response(start, collection, 'aggregate');

      throw new AmauiMongoError(error);
    }
  }

  public async searchMany(
    query: Query,
    additional: IMongoSearchManyAdditional = { pre: [], prePagination: [], post: [] },
    options: mongodb.AggregateOptions = {}
  ): Promise<IMongoResponse> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const projection = query.projection || this.projection;
      const sort = query.sort || this.sort as mongodb.Sort;
      const { limit, next, previous, skip } = query;
      const hasPaginator = next || previous;

      const pre = additional.pre || [];
      const pre_pagination = additional.prePagination || [];
      const post = additional.post || [];

      const queries = {
        search: query.queries.search[this.collectionName],
        api: query.queries.api[this.collectionName],
        permissions: query.queries.permissions[this.collectionName],
        aggregate: query.queries.aggregate[this.collectionName] || [],
      };

      const queryMongo = [
        ...pre,

        ...queries.aggregate,

        // Search
        ...(queries.search.length ? getMongoMatch(queries.search, query.settings.type) : []),

        // API
        ...(queries.api.length ? getMongoMatch(queries.api) : []),

        // Permissions
        ...(queries.permissions.length ? getMongoMatch(queries.permissions, '$or') : []),

        ...pre_pagination,
      ];

      const pipeline: any[] = [
        ...queryMongo,

        // Next paginator
        ...(next ? getMongoMatch([next as Record<string, any>]) : []),

        // Previous paginator
        ...(previous ? getMongoMatch([previous as Record<string, any>]) : []),

        ...(sort ? [{ $sort: sort }] : [{}]),

        // Either skip or a paginator
        ...((query.skip !== undefined && !hasPaginator) ? [{ $skip: skip }] : []),

        // +1 so we know if there's a next page
        { $limit: limit + 1 },

        ...(projection ? [{ $project: projection }] : []),

        ...post,
      ];

      const response_ = await collection.aggregate(pipeline, { ...Mongo.defaults.aggregateOptions, ...options }).toArray();

      // Add results and limit
      const objects = response_.slice(0, limit);
      const first = objects[0];
      const last = objects[objects.length - 1];

      const response = new MongoResponse(objects);

      response.sort = sort as any;
      response.skip = skip;
      response.limit = limit;

      // Add hasNext, next, previous
      response['hasNext'] = response_.length > objects.length;

      response['hasPrevious'] = !!(objects.length && (skip > 0 || next));

      if (last) response['next'] = AmauiMongo.createPaginator(last, [this.sortProperty], sort);

      if (first) response['previous'] = AmauiMongo.createPaginator(first, [this.sortProperty], sort, 'previous');

      // Count total only if it's requested by the query
      let total: number;

      if (query.total) {
        const total_ = await collection.aggregate(
          [
            ...queryMongo,

            // Limit count for performance reasons
            { $limit: Mongo.defaults.limitCount },

            { $group: { _id: null, count: { $sum: 1 } } },
          ],
          {
            ...Mongo.defaults.aggregateOptions,
            ...options
          }
        ).toArray();

        total = total_[0] && total_[0].count;

        response['total'] = total || 0;
      }

      return this.response(start, collection, 'searchMany', response);
    }
    catch (error) {
      this.response(start, collection, 'searchMany');

      throw new AmauiMongoError(error);
    }
  }

  public async searchOne(
    query: Query,
    additional: IMongoSearchOneAdditional = { pre: [], post: [] },
    options: mongodb.AggregateOptions = {}
  ): Promise<mongodb.Document> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const projection = query.projection || this.projection;
      const limit = 1;

      const pre = additional.pre || [];
      const post = additional.post || [];

      const queries = {
        search: query.queries.search[this.collectionName],
        api: query.queries.api[this.collectionName],
        permissions: query.queries.permissions[this.collectionName],
        aggregate: query.queries.aggregate[this.collectionName] || [],
      };

      const queryMongo = [
        ...pre,

        ...queries.aggregate,

        // Search
        ...(queries.search.length ? getMongoMatch(queries.search, query.settings.type) : []),

        // API
        ...(queries.api.length ? getMongoMatch(queries.api) : []),

        // Permissions
        ...(queries.permissions.length ? getMongoMatch(queries.permissions, '$or') : []),
      ];

      const pipeline: any[] = [
        ...queryMongo,

        { $limit: limit },

        ...(projection ? [{ $project: projection }] : []),

        ...post,
      ];

      const response = await collection.aggregate(
        pipeline,
        {
          ...Mongo.defaults.aggregateOptions,
          ...options
        }
      ).toArray();

      return this.response(start, collection, 'searchOne', response[0]);
    }
    catch (error) {
      this.response(start, collection, 'searchOne');

      throw new AmauiMongoError(error);
    }
  }

  public async addOne(
    value: any,
    options_: IAddOneOptions = {}
  ): Promise<mongodb.Document> {
    const options = { add_date: true, ...options_ };

    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      if (!value) throw new AmauiMongoError(`No value provided`);

      if (options.add_date) setObjectValue(value, this.addedProperty || 'api_meta.added_at', AmauiDate.utc.unix);

      const response = await collection.insertOne(value, options);

      return this.response(start, collection, 'addOne', { _id: response.insertedId, ...value });
    }
    catch (error) {
      this.response(start, collection, 'addOne');

      throw new AmauiMongoError(error);
    }
  }

  public async updateOne(
    query: Query,
    value?: any,
    operators: mongodb.UpdateFilter<any> = {},
    options_: IUpdateOptions = {}
  ): Promise<mongodb.ModifyResult<mongodb.Document>> {
    const options = { update_date: true, ...options_ };

    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      if (value !== undefined && !is('object', value)) throw new AmauiMongoError(`Value has to be an object with properties and values`);

      if (is('object', value) && options.update_date) value[this.updatedProperty || 'api_meta.updated_at'] = AmauiDate.utc.unix;

      const response = await collection.findOneAndUpdate(
        query.queries.find[this.collectionName],
        {
          ...(value ? { $set: value } : {}),
          ...operators,
        },
        {
          ...options,
          returnDocument: 'after',
        } as mongodb.FindOneAndUpdateOptions
      );

      return this.response(start, collection, 'updateOne', response.value);
    }
    catch (error) {
      this.response(start, collection, 'updateOne');

      throw new AmauiMongoError(error);
    }
  }

  public async removeOne(
    query: Query,
    options: mongodb.FindOneAndDeleteOptions = {}
  ): Promise<mongodb.ModifyResult<mongodb.Document>> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const response = await collection.findOneAndDelete(
        query.queries.find[this.collectionName],
        options
      );

      return this.response(start, collection, 'removeOne', response.value);
    }
    catch (error) {
      this.response(start, collection, 'removeOne');

      throw new AmauiMongoError(error);
    }
  }

  public async updateOneOrAdd(
    query: Query,
    value: any,
    options_: IUpdateOrAddOptions = {}
  ): Promise<mongodb.ModifyResult<mongodb.Document>> {
    const options = { add_date: true, update_date: true, ...options_ };

    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      if (!is('object', value)) throw new AmauiMongoError(`Value has to be an object with properties and values`);

      if (options.update_date) value[this.updatedProperty || 'api_meta.updated_at'] = AmauiDate.utc.unix;

      let setOnInsert: any;

      if (options.add_date) setOnInsert = {
        [this.addedProperty || 'api_meta.added_at']: AmauiDate.utc.unix
      };

      const response = await collection.findOneAndUpdate(
        query.queries.find[this.collectionName],
        {
          $set: value,
          ...(setOnInsert && { $setOnInsert: setOnInsert })
        },
        {
          ...options,
          upsert: true,
          returnDocument: 'after',
        }
      );

      return this.response(start, collection, 'updateOneOrAdd', response.value);
    }
    catch (error) {
      this.response(start, collection, 'updateOneOrAdd');

      throw new AmauiMongoError(error);
    }
  }

  public async addMany(
    values_: any[],
    options_: IAddManyOptions = {}
  ): Promise<Array<mongodb.Document>> {
    const options = { add_date: true, ...options_ };

    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      let values = values_;

      if (!values?.length) throw new AmauiMongoError(`Values have to be a non empty array`);

      if (options.add_date) values = values.map(item => {
        setObjectValue(item, this.addedProperty || 'api_meta.added_at', AmauiDate.utc.unix);

        return item;
      });

      const response = await collection.insertMany(
        values,
        {
          ordered: false,
          ...options
        }
      );

      return this.response(start, collection, 'addMany', response);
    }
    catch (error) {
      this.response(start, collection, 'addMany');

      throw new AmauiMongoError(error);
    }
  }

  public async updateMany(
    query: Query,
    value?: any,
    operators: mongodb.UpdateFilter<any> = {},
    options_: IUpdateManyOptions = {}
  ): Promise<number> {
    const options = { update_date: true, ...options_ };

    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      if (value !== undefined && !is('object', value)) throw new AmauiMongoError(`Value has to be an object with properties and values`);

      if (is('object', value) && options.update_date) value[this.updatedProperty || 'api_meta.updated_at'] = AmauiDate.utc.unix;

      const response = await collection.updateMany(
        query.queries.find[this.collectionName],
        {
          ...(value ? { $set: value } : {}),
          ...operators,
        },
        {
          ...options
        },
      );

      return this.response(start, collection, 'updateMany', response);
    }
    catch (error) {
      this.response(start, collection, 'updateMany');

      throw new AmauiMongoError(error);
    }
  }

  public async removeMany(
    query: Query,
    options: mongodb.DeleteOptions = { ordered: false }
  ): Promise<number> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const response = await collection.deleteMany(
        query.queries.find[this.collectionName],
        {
          ordered: false,
          ...options
        }
      );

      return this.response(start, collection, 'removeMany', response);
    }
    catch (error) {
      this.response(start, collection, 'removeMany');

      throw new AmauiMongoError(error);
    }
  }

  public async bulkWrite(
    values: mongodb.AnyBulkWriteOperation[] = [],
    options_: mongodb.BulkWriteOptions = {}
  ): Promise<Array<mongodb.Document>> {
    const options = { ...options_ };

    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      if (!values?.length) throw new AmauiMongoError(`Values have to be a non empty array`);

      const response = await collection.bulkWrite(
        values,
        {
          ordered: false,
          ...options
        }
      );

      return this.response(start, collection, 'bulkWrite', response);
    }
    catch (error) {
      this.response(start, collection, 'bulkWrite');

      throw new AmauiMongoError(error);
    }
  }

  protected toModel(value: any) {
    if (!this.Model) return value;

    return is('array', value) ? value.map(item => new this.Model(item)) : new this.Model(value);
  }

  protected response(
    start: number,
    collection: mongodb.Collection,
    method: string,
    value?: any,
    req?: express.Request
  ): any {
    if (is('number', start)) {
      const arguments_ = [];

      if (collection) arguments_.push(`Collection: ${collection.collectionName}`);
      if (method) arguments_.push(`Method: ${method}`);
      if ((req as any)?.id) arguments_.push(`Request ID: ${(req as any).id}`);

      arguments_.push(`Duration: ${duration(AmauiDate.utc.milliseconds - start, true)}`);

      this.amalog.debug(...arguments_);
    }

    if (this.Model !== undefined) {
      switch (method) {
        case 'find':
        case 'searchMany':
          (value as MongoResponse).response = this.toModel((value as MongoResponse).response);

          break;

        case 'findOne':
        case 'aggregate':
        case 'searchOne':
        case 'addOne':
        case 'updateOne':
        case 'removeOne':
        case 'updateOneOrAdd':
          return this.toModel(value);

        default:
          break;
      }
    }

    return value;
  }

}

export default BaseCollection;
