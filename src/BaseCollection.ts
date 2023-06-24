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

export interface IFindOptions extends mongodb.FindOptions {
  total?: boolean;
  sort?: any;
  projection?: any;
}

export interface ISearchOne extends mongodb.AggregateOptions {
  projection?: any;
}

export interface ISearchManyOptions extends mongodb.AggregateOptions {
  total?: boolean;
  limit?: number;
  skip?: number;
  sort?: any;
  next?: any;
  previous?: any;
  projection?: any;
}

export type TMethods = 'count' | 'exists' | 'find' | 'findOne' | 'aggregate' | 'searchMany' | 'searchOne' | 'addOne' | 'updateOne' | 'removeOne' | 'updateOneOrAdd' | 'addMany' | 'updateMany' | 'removeMany' | 'bulkWrite';

export type TDefaultProperties = 'query' | 'queryObject' | 'queryArray';

export type TDefaults = {
  [p in TDefaultProperties | TMethods]: any;
}

export class BaseCollection {
  private db_: mongodb.Db;
  protected collections: Record<string, mongodb.Collection> = {};
  protected amalog: AmauiLog;

  public static defaults: TDefaults;

  public constructor(
    protected collectionName: string,
    public mongo: Mongo,
    public Model?: IClass,
    public defaults?: TDefaults
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

  public get sortProperty(): string { return 'added_at'; }

  public get sortAscending(): number { return -1; }

  public get addedProperty(): string { return 'added_at'; }

  public get updatedProperty(): string { return 'updated_at'; }

  public get projection(): object { return; }

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
    query: any = new Query(),
    options: mongodb.CountDocumentsOptions = {}
  ): Promise<number> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const defaults = this.getDefaults('count') as any;

      const response = await collection.countDocuments(
        {
          // defaults
          ...defaults,

          ...this.query(query)
        },
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
    query: any,
    options: mongodb.FindOptions = {}
  ): Promise<boolean> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const defaults = this.getDefaults('exists') as any;

      const response = await collection.findOne(
        {
          // defaults
          ...defaults,

          ...this.query(query)
        },
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
    query: any,
    options: IFindOptions = {}
  ): Promise<IMongoResponse> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const {
        total,
        sort,
        limit,
        skip,

        ...optionsOther
      } = options;

      const defaults = this.getDefaults('find') as any;

      const optionsMongo: any = { ...optionsOther };

      if (!optionsMongo.projection) {
        optionsMongo.projection = (BaseCollection.isAmauiQuery(query) && query.projection) || this.projection;

        if (!optionsMongo.projection) delete optionsMongo.projection;
      }
      optionsMongo.sort = ((BaseCollection.isAmauiQuery(query) ? query.sort : sort) || this.sort as mongodb.Sort);
      optionsMongo.skip = (BaseCollection.isAmauiQuery(query) ? query.skip : skip) || 0;
      optionsMongo.limit = (BaseCollection.isAmauiQuery(query) ? query.limit : limit) || 15;

      const queryMongo = {
        // defaults
        ...defaults,

        ...this.query(query)
      };

      const response_ = await collection.find(
        queryMongo,
        optionsMongo
      ).toArray();

      const response = new MongoResponse(response_);

      response.sort = optionsMongo.sort as any;
      response.size = response_.length;
      response.skip = optionsMongo.skip;
      response.limit = optionsMongo.limit;

      if (BaseCollection.isAmauiQuery(query) ? query.total : total) {
        response['total'] = await collection.find(
          queryMongo,
          { projection: { _id: 1 } }
        ).count();
      }

      return this.response(start, collection, 'find', response);
    }
    catch (error) {
      this.response(start, collection, 'find');

      throw new AmauiMongoError(error);
    }
  }

  public async findOne(
    query: any,
    options: mongodb.FindOptions = {}
  ): Promise<any> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const defaults = this.getDefaults('findOne') as any;

      if (!options.projection) {
        options.projection = (BaseCollection.isAmauiQuery(query) && query.projection) || this.projection;

        if (!options.projection) delete options.projection;
      }

      const response = await collection.findOne(
        {
          // defaults
          ...defaults,

          ...this.query(query)
        },
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
    query: any = new Query(),
    options: mongodb.AggregateOptions = {}
  ): Promise<Array<mongodb.Document>> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const defaults = this.getDefaults('aggregate') as any;

      const response = collection.aggregate(
        [
          // defaults
          ...(defaults || []),

          ...this.query(query)
        ],
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
    query: any,
    additional: IMongoSearchManyAdditional = { pre: [], prePagination: [], post: [] },
    options: ISearchManyOptions = {}
  ): Promise<IMongoResponse> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const {
        total: optionsTotal,
        sort: optionsSort,
        limit: optionsLimit = 15,
        skip: optionsSkip,
        projection: optionsProjection,

        ...optionsOther
      } = options;

      const defaults = this.getDefaults('searchMany') as any;

      const optionsMongo = { ...optionsOther };

      const projection = (BaseCollection.isAmauiQuery(query) ? query.projection : optionsProjection) || this.projection;
      const sort = (BaseCollection.isAmauiQuery(query) ? query.sort : optionsSort) || this.sort as mongodb.Sort;
      const {
        limit = optionsLimit,
        skip = optionsSkip,
        next,
        previous
      } = BaseCollection.isAmauiQuery(query) ? query : options;
      const hasPaginator = next || previous;

      const pre = additional.pre || [];
      const pre_pagination = additional.prePagination || [];
      const post = additional.post || [];

      const queries = {
        search: (BaseCollection.isAmauiQuery(query) ? query.queries.search[this.collectionName] : []) || [],
        api: (BaseCollection.isAmauiQuery(query) ? query.queries.api[this.collectionName] : []) || [],
        permissions: (BaseCollection.isAmauiQuery(query) ? query.queries.permissions[this.collectionName] : []) || [],
        aggregate: (BaseCollection.isAmauiQuery(query) ? query.queries.aggregate[this.collectionName] : []) || []
      };

      const queryMongo = [
        // defaults
        ...(defaults || []),

        ...((BaseCollection.isAmauiQuery(query) ? query.query : query) || []),

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
        ...((query?.skip !== undefined && !hasPaginator) ? [{ $skip: skip }] : []),

        // +1 so we know if there's a next page
        { $limit: limit + 1 },

        ...(projection ? [{ $project: projection }] : []),

        ...post,
      ];

      const response_ = await collection.aggregate(
        pipeline,
        {
          ...Mongo.defaults.aggregateOptions,
          ...optionsMongo
        }
      ).toArray();

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

      if (BaseCollection.isAmauiQuery(query) ? query.total : optionsTotal) {
        const total_ = await collection.aggregate(
          [
            ...queryMongo,

            // Limit count for performance reasons
            { $limit: Mongo.defaults.limitCount },

            { $group: { _id: null, count: { $sum: 1 } } },
          ],
          {
            ...Mongo.defaults.aggregateOptions,
            ...optionsMongo
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
    query: any,
    additional: IMongoSearchOneAdditional = { pre: [], post: [] },
    options: ISearchOne = {}
  ): Promise<mongodb.Document> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const {
        projection: optionsProjection,

        ...optionsOther
      } = options;

      const defaults = this.getDefaults('searchOne') as any;

      const optionsMongo = { ...optionsOther };

      const limit = 1;
      const projection = (BaseCollection.isAmauiQuery(query) ? query.projection : optionsProjection) || this.projection;

      const pre = additional.pre || [];
      const post = additional.post || [];

      const queries = {
        search: (BaseCollection.isAmauiQuery(query) ? query.queries.search[this.collectionName] : []) || [],
        api: (BaseCollection.isAmauiQuery(query) ? query.queries.api[this.collectionName] : []) || [],
        permissions: (BaseCollection.isAmauiQuery(query) ? query.queries.permissions[this.collectionName] : []) || [],
        aggregate: (BaseCollection.isAmauiQuery(query) ? query.queries.aggregate[this.collectionName] : []) || []
      };

      const queryMongo = [
        // defaults
        ...(defaults || []),

        ...((BaseCollection.isAmauiQuery(query) ? query.query : query) || []),

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
          ...optionsMongo
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

    const {
      add_date,

      ...optionsMongo
    } = options;

    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      if (!value) throw new AmauiMongoError(`No value provided`);

      if (add_date) setObjectValue(value, this.addedProperty || 'added_at', AmauiDate.utc.unix);

      const response = await collection.insertOne(value, optionsMongo);

      return this.response(start, collection, 'addOne', { _id: response.insertedId, ...value });
    }
    catch (error) {
      this.response(start, collection, 'addOne');

      throw new AmauiMongoError(error);
    }
  }

  public async updateOne(
    query: any,
    value?: any,
    operators: mongodb.UpdateFilter<any> = {},
    options_: IUpdateOptions = {}
  ): Promise<mongodb.ModifyResult<mongodb.Document>> {
    const options = { update_date: true, ...options_ };

    const {
      update_date,

      ...optionsMongo
    } = options;

    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const defaults = this.getDefaults('updateOne') as any;

      if (value !== undefined && !is('object', value)) throw new AmauiMongoError(`Value has to be an object with properties and values`);

      if (is('object', value) && update_date) value[this.updatedProperty || 'updated_at'] = AmauiDate.utc.unix;

      const response = await collection.findOneAndUpdate(
        {
          // defaults
          ...defaults,

          ...this.query(query)
        },
        {
          ...(value ? { $set: value } : {}),

          ...operators
        },
        {
          returnDocument: 'after',

          ...optionsMongo
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
    query: any,
    options: mongodb.FindOneAndDeleteOptions = {}
  ): Promise<mongodb.ModifyResult<mongodb.Document>> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const defaults = this.getDefaults('removeOne') as any;

      const response = await collection.findOneAndDelete(
        {
          // defaults
          ...defaults,

          ...this.query(query)
        },
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
    query: any,
    value: any,
    options_: IUpdateOrAddOptions = {}
  ): Promise<mongodb.ModifyResult<mongodb.Document>> {
    const options = { add_date: true, update_date: true, ...options_ };

    const {
      add_date,
      update_date,

      ...optionsMongo
    } = options;

    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const defaults = this.getDefaults('updateOneOrAdd') as any;

      if (!is('object', value)) throw new AmauiMongoError(`Value has to be an object with properties and values`);

      if (update_date) value[this.updatedProperty || 'updated_at'] = AmauiDate.utc.unix;

      let setOnInsert: any;

      if (add_date) setOnInsert = {
        [this.addedProperty || 'added_at']: AmauiDate.utc.unix
      };

      const response = await collection.findOneAndUpdate(
        {
          // defaults
          ...defaults,

          ...this.query(query)
        },
        {
          $set: value,
          ...(setOnInsert && { $setOnInsert: setOnInsert })
        },
        {
          upsert: true,
          returnDocument: 'after',

          ...optionsMongo
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

    const {
      add_date,

      ...optionsMongo
    } = options;

    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      let values = values_;

      if (!values?.length) throw new AmauiMongoError(`Values have to be a non empty array`);

      if (add_date) values = values.map(item => {
        setObjectValue(item, this.addedProperty || 'added_at', AmauiDate.utc.unix);

        return item;
      });

      const response = await collection.insertMany(
        values,
        {
          ordered: false,

          ...optionsMongo
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
    query: any,
    value?: any,
    operators: mongodb.UpdateFilter<any> = {},
    options_: IUpdateManyOptions = {}
  ): Promise<number> {
    const options = { update_date: true, ...options_ };

    const {
      update_date,

      ...optionsMongo
    } = options;

    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const defaults = this.getDefaults('updateMany') as any;

      if (value !== undefined && !is('object', value)) throw new AmauiMongoError(`Value has to be an object with properties and values`);

      if (is('object', value) && update_date) value[this.updatedProperty || 'updated_at'] = AmauiDate.utc.unix;

      const response = await collection.updateMany(
        {
          // defaults
          ...defaults,

          ...this.query(query)
        },
        {
          ...(value ? { $set: value } : {}),

          ...operators,
        },
        {
          ...optionsMongo
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
    query: any,
    options: mongodb.DeleteOptions = {}
  ): Promise<number> {
    const collection = await this.collection();
    const start = AmauiDate.utc.milliseconds;

    try {
      const defaults = this.getDefaults('removeMany') as any;

      const response = await collection.deleteMany(
        {
          // defaults
          ...defaults,

          ...this.query(query)
        },
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

  public query(query: any, aggregate = false) {
    if (BaseCollection.isAmauiQuery(query)) {
      if (aggregate) {
        return [
          ...(query?.query || []),

          ...(query?.queries?.aggregate?.[this.collectionName] || [])
        ];
      }
      else {
        return {
          ...query?.query,

          ...query?.queries?.find?.[this.collectionName]
        };
      }
    }

    return aggregate ? (query || []) : query;
  }

  public getDefaults(method: TMethods) {
    let value = ['aggregate', 'searchMany', 'searchOne'].includes(method) ? [] : {};

    // static
    if (['aggregate', 'searchMany', 'searchOne'].includes(method)) {
      // query
      if (is('array', BaseCollection.defaults?.query)) (value as any[]).push(...BaseCollection.defaults?.query);

      // queryArray
      if (is('array', BaseCollection.defaults?.queryArray)) (value as any[]).push(...BaseCollection.defaults?.queryArray);

      // method
      if (is('array', BaseCollection.defaults?.[method])) (value as any[]).push(...BaseCollection.defaults?.[method]);
    }
    else {
      // query
      if (is('object', BaseCollection.defaults?.query)) value = { ...value, ...BaseCollection.defaults?.query };

      // queryObject
      if (is('object', BaseCollection.defaults?.queryObject)) value = { ...value, ...BaseCollection.defaults?.queryObject };

      // method
      if (is('object', BaseCollection.defaults?.[method])) value = { ...value, ...BaseCollection.defaults?.[method] };
    }

    // instance
    if (['aggregate', 'searchMany', 'searchOne'].includes(method)) {
      // query
      if (is('array', this.defaults?.query)) (value as any[]).push(...this.defaults?.query);

      // queryArray
      if (is('array', this.defaults?.queryArray)) (value as any[]).push(...this.defaults?.queryArray);

      // method
      if (is('array', this.defaults?.[method])) (value as any[]).push(...this.defaults?.[method]);
    }
    else {
      // query
      if (is('object', this.defaults?.query)) value = { ...value, ...this.defaults?.query };

      // queryObject
      if (is('object', this.defaults?.queryObject)) value = { ...value, ...this.defaults?.queryObject };

      // method
      if (is('object', this.defaults?.[method])) value = { ...value, ...this.defaults?.[method] };
    }

  }

  public static isAmauiQuery(value: any) {
    return value instanceof Query || (value?.hasOwnProperty('query') && value?.hasOwnProperty('queries'));
  }

}

export default BaseCollection;
