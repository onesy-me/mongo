import * as mongodb from 'mongodb';
import express from 'express';

import is from '@onesy/utils/is';
import copy from '@onesy/utils/copy';
import wait from '@onesy/utils/wait';
import getObjectValue from '@onesy/utils/getObjectValue';
import setObjectValue from '@onesy/utils/setObjectValue';
import { TMethod, Query, IMongoResponse, getMongoMatch, IMongoSearchManyAdditional as IMongoSearchManyAdditionalInterface, IMongoSearchOneAdditional as IMongoSearchOneAdditionalInterface, MongoResponse, IClass, } from '@onesy/models';
import { OnesyMongoError, DeveloperError } from '@onesy/errors';
import OnesyDate from '@onesy/date/OnesyDate';
import duration from '@onesy/date/duration';
import OnesyLog from '@onesy/log';

import Mongo from './Mongo';
import OnesyMongo from './OnesyMongo';

export type IMongoLookup = {
  property?: string;
  query?: any;
  projection?: any;
  override?: string[];
  options?: mongodb.AggregateOptions;
  objects: BaseCollection<any>;
  toObjectResponse?: boolean;
}

export type IMongoOption = {
  name: string;
  property: string;
  version?: 'array' | 'object' | 'string' | 'objectID';
  lookup?: IMongoLookup;
}

export interface IMongoSearchManyAdditional extends IMongoSearchManyAdditionalInterface {
  lookups?: IMongoLookup[];
  options?: IMongoOption[];
}

export interface IMongoSearchOneAdditional extends IMongoSearchOneAdditionalInterface {
  lookups?: IMongoLookup[];
}

export interface IUpdateFilters extends mongodb.UpdateFilter<unknown> {
  [p: string]: any;
}

export interface IUpdateOrAddOptions extends mongodb.FindOneAndUpdateOptions {
  add_date?: boolean;
  update_date?: boolean;
  request?: any;
}

export interface IUpdateOptions extends mongodb.FindOneAndUpdateOptions {
  lookups?: IMongoLookup[];
  update_date?: boolean;
  request?: any;
}

export interface IUpdateManyOptions extends mongodb.UpdateOptions {
  update_date?: boolean;
  request?: any;
}

export interface IAddOneOptions extends mongodb.InsertOneOptions {
  add_date?: boolean;
  request?: any;
}

export interface IAddManyOptions extends mongodb.BulkWriteOptions {
  original?: boolean;
  add_date?: boolean;
  request?: any;
}

export interface IFindOptions extends mongodb.FindOptions {
  total?: boolean;
  sort?: any;
  projection?: any;
  request?: any;
}

export interface ISearchOne extends mongodb.AggregateOptions {
  projection?: any;
  request?: any;
}

export interface ISearchManyOptions extends mongodb.AggregateOptions {
  total?: boolean;
  limit?: number;
  skip?: number;
  sort?: any;
  next?: any;
  previous?: any;
  projection?: any;
  request?: any;
}

export interface IAggregateOptions extends mongodb.AggregateOptions {
  request?: any;
}

export interface IRemoveOneOptions extends mongodb.FindOneAndDeleteOptions {
  request?: any;
}

export interface IRemoveManyOptions extends mongodb.DeleteOptions {
  request?: any;
}

export type TMethods = 'count' | 'exists' | 'find' | 'findOne' | 'aggregate' | 'searchMany' | 'searchOne' | 'addOne' | 'updateOne' | 'removeOne' | 'updateOneOrAdd' | 'addMany' | 'updateMany' | 'removeMany' | 'bulkWrite';

export type TDefaultProperties = 'query' | 'queryObject' | 'queryArray';

export type TDefaults = {
  [p in TDefaultProperties | TMethods]: any;
}

export class BaseCollection<IModel = any> {
  protected collections: Record<string, mongodb.Collection> = {};
  protected onesyLog: OnesyLog;

  public static defaults: TDefaults;

  public constructor(
    protected collectionName: string,
    public mongo: Mongo,
    public Model?: IClass,
    public defaults?: TDefaults
  ) {
    if (!(mongo && mongo instanceof Mongo)) throw new OnesyMongoError(`Mongo instance is required`);
    if (!collectionName) throw new OnesyMongoError(`Collection name is required`);

    // log inherit from Mongo
    // so it can be configured on per use basis
    this.onesyLog = mongo.onesyLog;
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
      const db = await this.mongo.connection as mongodb.Db;

      return resolve(db);
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

      this.onesyLog.info(`${this.collectionName} collection created`);
    }

    return this.collections[name];
  }

  public async transaction(method: TMethod, options = { retries: 5, retriesWait: 140 }): Promise<any> {
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
        await session.withTransaction(async () => {
          response = await method(session);

          return response;
        }, transactionOptions);
      }
      catch (error_) {
        error = error_;
        codeName = error_.codeName || error_.message?.codeName;

        this.onesyLog.error('session error', error.name, (error as any).code, error.message, error.stack);
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
    const start = OnesyDate.utc.milliseconds;

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

      throw new OnesyMongoError(error);
    }
  }

  public async exists(
    query: any,
    options: mongodb.FindOptions = {}
  ): Promise<boolean> {
    const collection = await this.collection();
    const start = OnesyDate.utc.milliseconds;

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

      throw new OnesyMongoError(error);
    }
  }

  public async find(
    query: any,
    options: IFindOptions = {}
  ): Promise<IMongoResponse> {
    const collection = await this.collection();
    const start = OnesyDate.utc.milliseconds;

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
        optionsMongo.projection = (BaseCollection.isOnesyQuery(query) && query.projection) || this.projection;

        if (!optionsMongo.projection) delete optionsMongo.projection;
      }
      optionsMongo.sort = ((BaseCollection.isOnesyQuery(query) ? query.sort : sort) || this.sort as mongodb.Sort);
      optionsMongo.skip = (BaseCollection.isOnesyQuery(query) ? query.skip : skip) || 0;
      optionsMongo.limit = (BaseCollection.isOnesyQuery(query) ? query.limit : limit) || 15;

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

      if (BaseCollection.isOnesyQuery(query) ? query.total : total) {
        response['total'] = await collection.find(
          queryMongo,
          { projection: { _id: 1 } }
        ).count();
      }

      return this.response(start, collection, 'find', response);
    }
    catch (error) {
      this.response(start, collection, 'find');

      throw new OnesyMongoError(error);
    }
  }

  public async findOne(
    query: any,
    options: mongodb.FindOptions = {}
  ): Promise<IModel> {
    const collection = await this.collection();
    const start = OnesyDate.utc.milliseconds;

    try {
      const defaults = this.getDefaults('findOne') as any;

      if (!options.projection) {
        options.projection = (BaseCollection.isOnesyQuery(query) && query.projection) || this.projection;

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

      throw new OnesyMongoError(error);
    }
  }

  public async aggregate(
    query: any = new Query(),
    options: IAggregateOptions = {}
  ): Promise<Array<IModel>> {
    const collection = await this.collection();
    const start = OnesyDate.utc.milliseconds;

    try {
      const defaults = this.getDefaults('aggregate') as any;

      const response = await collection.aggregate(
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

      throw new OnesyMongoError(error);
    }
  }

  public async searchMany(
    query: any,
    additional: IMongoSearchManyAdditional = { pre: [], prePagination: [], post: [], options: [], lookups: [] },
    options: ISearchManyOptions = {}
  ): Promise<IMongoResponse> {
    const collection = await this.collection();
    const start = OnesyDate.utc.milliseconds;

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

      const projection = (BaseCollection.isOnesyQuery(query) ? query.projection : optionsProjection) || this.projection;
      const sort = (BaseCollection.isOnesyQuery(query) ? query.sort : optionsSort) || this.sort as mongodb.Sort;
      const {
        limit = optionsLimit,
        skip = optionsSkip,
        next,
        previous
      } = BaseCollection.isOnesyQuery(query) ? query : options;
      const hasPaginator = next || previous;

      const paginatorProperty = Object.keys((next || previous || {}))[0];

      const pre = additional.pre || [];
      const pre_pagination = additional.prePagination || [];
      const post = additional.post || [];

      const queries = {
        search: (BaseCollection.isOnesyQuery(query) ? query.queries.search[this.collectionName] : []) || [],
        api: (BaseCollection.isOnesyQuery(query) ? query.queries.api[this.collectionName] : []) || [],
        permissions: (BaseCollection.isOnesyQuery(query) ? query.queries.permissions[this.collectionName] : []) || [],
        aggregate: (BaseCollection.isOnesyQuery(query) ? query.queries.aggregate[this.collectionName] : []) || []
      };

      const queryMongo = [
        // defaults
        ...(defaults || []),

        ...((BaseCollection.isOnesyQuery(query) ? query.query : query) || []),

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

        ...(hasPaginator ? [{ $sort: { [paginatorProperty]: next ? -1 : 1 } }] : []),

        ...(sort && !hasPaginator ? [{ $sort: sort }] : []),

        // Either skip or a paginator
        ...((query?.skip !== undefined && !hasPaginator) ? [{ $skip: skip }] : []),

        // +1 so we know if there's a next page
        { $limit: limit + 1 },

        ...(sort && hasPaginator ? [{ $sort: sort }] : []),

        ...(projection ? [{ $project: projection }] : []),

        ...post
      ];

      const response_ = await collection.aggregate(
        pipeline,
        {
          ...Mongo.defaults.aggregateOptions,
          ...optionsMongo
        }
      ).toArray();

      // Add results and limit
      const objects = (!hasPaginator || next || response_.length <= limit) ? response_.slice(0, limit) : response_.slice(1, limit + 1);
      const first = objects[0];
      const last = objects[objects.length - 1];

      const response = new MongoResponse(objects);

      response.sort = sort as any;
      response.skip = skip;
      response.limit = limit;

      // Add hasNext, next, previous
      response['hasNext'] = previous || response_.length > objects.length;

      response['hasPrevious'] = !hasPaginator ? query.skip > 0 : next || (response_.length > objects.length);

      if (last) response['next'] = OnesyMongo.createPaginator(last, [this.sortProperty], sort);

      if (first) response['previous'] = OnesyMongo.createPaginator(first, [this.sortProperty], sort, 'previous');

      // lookups
      await this.lookups(response.response, additional.lookups, options.request);

      // options
      if (!!additional.options?.length) {
        const optionsResponse = await collection.aggregate(
          [
            ...queryMongo,

            ...additional.options.map(item => ['array', undefined].includes(item.version) ? ({
              $unwind: `$${item.property}`
            }) : undefined).filter(Boolean),

            ...additional.options.map(item => ({
              $group: {
                _id: item.name,

                value: {
                  $addToSet: item.property.startsWith('$') ? item.property : `$${item.property}`
                }
              }
            }))
          ],
          {
            ...Mongo.defaults.aggregateOptions,
            ...optionsMongo
          }
        ).toArray();

        const optionsMongoResponse = {};

        optionsResponse.forEach(item => optionsMongoResponse[item._id] = item.value?.flatMap(item => item) || []);

        for (const optionName of Object.keys(optionsMongoResponse)) {
          const optionRequest = additional.options.find(item => item.name === optionName);

          if (optionRequest.lookup) await this.lookups(optionsMongoResponse[optionName], [optionRequest.lookup], options.request);
        }

        response.options = optionsMongoResponse;
      }

      // Count total only if it's requested by the query
      let total: number;

      if (BaseCollection.isOnesyQuery(query) ? query.total : optionsTotal) {
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

      throw new OnesyMongoError(error);
    }
  }

  public async searchOne(
    query: any,
    additional: IMongoSearchOneAdditional = { pre: [], post: [], lookups: [] },
    options: ISearchOne = {}
  ): Promise<IModel> {
    const collection = await this.collection();
    const start = OnesyDate.utc.milliseconds;

    try {
      const {
        projection: optionsProjection,

        ...optionsOther
      } = options;

      const defaults = this.getDefaults('searchOne') as any;

      const optionsMongo = { ...optionsOther };

      const limit = 1;
      const projection = (BaseCollection.isOnesyQuery(query) ? query.projection : optionsProjection) || this.projection;

      const pre = additional.pre || [];
      const post = additional.post || [];

      const queries = {
        search: (BaseCollection.isOnesyQuery(query) ? query.queries.search[this.collectionName] : []) || [],
        api: (BaseCollection.isOnesyQuery(query) ? query.queries.api[this.collectionName] : []) || [],
        permissions: (BaseCollection.isOnesyQuery(query) ? query.queries.permissions[this.collectionName] : []) || [],
        aggregate: (BaseCollection.isOnesyQuery(query) ? query.queries.aggregate[this.collectionName] : []) || []
      };

      const queryMongo = [
        // defaults
        ...(defaults || []),

        ...((BaseCollection.isOnesyQuery(query) ? query.query : query) || []),

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

      // lookups
      await this.lookups(response, additional.lookups, options.request);

      return this.response(start, collection, 'searchOne', response[0]);
    }
    catch (error) {
      this.response(start, collection, 'searchOne');

      throw new OnesyMongoError(error);
    }
  }

  public async addOne(
    value_: any,
    options_: IAddOneOptions = {}
  ): Promise<IModel> {
    const options = { add_date: true, ...options_ };

    const {
      add_date,

      ...optionsMongo
    } = options;

    const collection = await this.collection();
    const start = OnesyDate.utc.milliseconds;

    try {
      const value = BaseCollection.value(value_);

      if (!value) throw new OnesyMongoError(`No value provided`);

      if (add_date) setObjectValue(value, this.addedProperty || 'added_at', OnesyDate.utc.milliseconds);

      const response = await collection.insertOne(value, optionsMongo);

      return this.response(start, collection, 'addOne', { _id: response.insertedId, ...value });
    }
    catch (error) {
      this.response(start, collection, 'addOne');

      throw new OnesyMongoError(error);
    }
  }

  public async updateOne(
    query: any,
    value: IUpdateFilters,
    options_: IUpdateOptions = { lookups: [] }
  ): Promise<IModel> {
    const options = { update_date: true, ...options_ };

    const {
      update_date,

      ...optionsMongo
    } = options;

    const collection = await this.collection();
    const start = OnesyDate.utc.milliseconds;

    try {
      const defaults = this.getDefaults('updateOne') as any;

      if (value !== undefined && !is('object', value)) throw new OnesyMongoError(`Value has to be an object with update values`);

      if (is('object', value) && update_date) value[this.updatedProperty || 'updated_at'] = OnesyDate.utc.milliseconds;

      const update = {};
      const operators = {};

      // use both update values
      // and operator values in the same object
      Object.keys(value).forEach(item => {
        if (item.startsWith('$')) operators[item] = value[item];
        else update[item] = value[item];
      });

      const response = await collection.findOneAndUpdate(
        {
          // defaults
          ...defaults,

          ...this.query(query)
        },
        {
          ...operators,

          ...((!!Object.keys(update).length || operators['$set']) && {
            $set: {
              ...operators['$set'],

              ...update
            }
          })
        },
        {
          returnDocument: 'after',

          ...optionsMongo
        } as mongodb.FindOneAndUpdateOptions
      );

      // lookups
      await this.lookups(response.value, options.lookups, options.request);

      return this.response(start, collection, 'updateOne', response.value);
    }
    catch (error) {
      this.response(start, collection, 'updateOne');

      throw new OnesyMongoError(error);
    }
  }

  public async removeOne(
    query: any,
    options: IRemoveOneOptions = {}
  ): Promise<mongodb.ModifyResult<IModel>> {
    const collection = await this.collection();
    const start = OnesyDate.utc.milliseconds;

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

      throw new OnesyMongoError(error);
    }
  }

  public async updateOneOrAdd(
    query: any,
    value: any,
    options_: IUpdateOrAddOptions = {}
  ): Promise<IModel> {
    const options = { add_date: true, update_date: true, ...options_ };

    const {
      add_date,
      update_date,

      ...optionsMongo
    } = options;

    const collection = await this.collection();
    const start = OnesyDate.utc.milliseconds;

    try {
      const defaults = this.getDefaults('updateOneOrAdd') as any;

      if (!is('object', value)) throw new OnesyMongoError(`Value has to be an object with properties and values`);

      if (update_date) value[this.updatedProperty || 'updated_at'] = OnesyDate.utc.milliseconds;

      let setOnInsert: any;

      if (add_date) setOnInsert = {
        [this.addedProperty || 'added_at']: OnesyDate.utc.milliseconds
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

      throw new OnesyMongoError(error);
    }
  }

  public async addMany(
    values_: any[],
    options_: IAddManyOptions = {}
  ): Promise<Array<IModel>> {
    const options = { add_date: true, ...options_ };

    const {
      add_date,

      ...optionsMongo
    } = options;

    const collection = await this.collection();
    const start = OnesyDate.utc.milliseconds;

    try {
      let values = values_.map(item => BaseCollection.value(item));

      if (!values?.length) throw new OnesyMongoError(`Values have to be a non empty array`);

      if (add_date) values = values.map(item => {
        setObjectValue(item, this.addedProperty || 'added_at', OnesyDate.utc.milliseconds);

        return item;
      });

      let response = await collection.insertMany(
        values,
        {
          ordered: false,

          ...optionsMongo
        }
      );

      if (!options.original) {
        const ids = Object.keys(response.insertedIds || {}).map(item => response.insertedIds?.[item]);

        response = values.filter(item => !!ids.find(id => item._id.toString() === id.toString())) as any;
      }

      return this.response(start, collection, 'addMany', response);
    }
    catch (error) {
      this.response(start, collection, 'addMany');

      throw new OnesyMongoError(error);
    }
  }

  public async updateMany(
    query: any,
    value: IUpdateFilters,
    options_: IUpdateManyOptions = {}
  ): Promise<number> {
    const options = { update_date: true, ...options_ };

    const {
      update_date,

      ...optionsMongo
    } = options;

    const collection = await this.collection();
    const start = OnesyDate.utc.milliseconds;

    try {
      const defaults = this.getDefaults('updateMany') as any;

      if (value !== undefined && !is('object', value)) throw new OnesyMongoError(`Value has to be an object with properties and values`);

      if (is('object', value) && update_date) value[this.updatedProperty || 'updated_at'] = OnesyDate.utc.milliseconds;

      const update = {};
      const operators = {};

      // use both update values
      // and operator values in the same object
      Object.keys(value).forEach(item => {
        if (item.startsWith('$')) operators[item] = value[item];
        else update[item] = value[item];
      });

      const response = await collection.updateMany(
        {
          // defaults
          ...defaults,

          ...this.query(query)
        },
        {
          ...operators,

          ...((!!Object.keys(update).length || operators['$set']) && {
            $set: {
              ...operators['$set'],

              ...update
            }
          })
        },
        {
          ...optionsMongo
        }
      );

      return this.response(start, collection, 'updateMany', response);
    }
    catch (error) {
      this.response(start, collection, 'updateMany');

      throw new OnesyMongoError(error);
    }
  }

  public async removeMany(
    query: any,
    options: IRemoveManyOptions = {}
  ): Promise<number> {
    const collection = await this.collection();
    const start = OnesyDate.utc.milliseconds;

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

      throw new OnesyMongoError(error);
    }
  }

  public async bulkWrite(
    values: mongodb.AnyBulkWriteOperation[] = [],
    options_: mongodb.BulkWriteOptions = {}
  ): Promise<mongodb.BulkWriteResult> {
    const options = { ...options_ };

    const collection = await this.collection();
    const start = OnesyDate.utc.milliseconds;

    try {
      if (!values?.length) throw new OnesyMongoError(`Values have to be a non empty array`);

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

      throw new OnesyMongoError(error);
    }
  }

  public async lookups(value_: any, lookups: IMongoLookup[], request: any) {
    const value = is('array', value_) ? value_ : [value_];

    if (!!value.length && !!lookups?.length) {
      for (const lookup of lookups) {
        try {
          if (lookup.objects) {
            const ids = [];

            // Get the ids to lookup
            value.forEach(item => {
              const valueProperty = !lookup.property ? item : getObjectValue(item, lookup.property);

              ids.push(...this.getLookupIDs(valueProperty));
            });

            if (!!ids.length) {
              // Search objects
              const query = lookup.query || [];

              if (BaseCollection.isOnesyQuery(query)) {
                if (is('array', (query as Query).query)) {
                  ((query as Query).query as any[]).unshift(
                    {
                      $match: {
                        _id: { $in: ids }
                      }
                    }
                  );

                  if (lookup.projection) {
                    ((query as Query).query as any[]).push({
                      $project: {
                        ...lookup.projection
                      }
                    });
                  }
                }
              }
              else {
                (query as any[]).unshift(
                  {
                    $match: {
                      _id: { $in: ids }
                    }
                  }
                );

                if (lookup.projection) {
                  (query as any[]).push({
                    $project: {
                      ...lookup.projection
                    }
                  });
                }
              }

              const method = lookup.objects.aggregate.bind(lookup.objects);

              let response = await method(query, lookup.options);

              if ([true, undefined].includes(lookup.toObjectResponse)) {
                response = response.map(item => {
                  if (item.toObjectResponse) return item.toObjectResponse(request);

                  return item;
                });
              }

              const responseMap = {};

              response.forEach(item => {
                responseMap[(item._id || item.id).toString()] = item;
              });

              // Update all the id objects
              value.forEach((item, index: number) => {
                const valueItem = this.updateLookupProperty(item, item, responseMap, lookup);

                if (!lookup.property && valueItem) value[index] = valueItem;
              });
            }
          }
        }
        catch (error) {
          console.error(`Lookups error`, error);
        }
      }
    }
  }

  public updateLookupProperty(mongoObject: any, object: any, responseMap: any, lookup: IMongoLookup, array = false) {
    const valueProperty = array ? object : !lookup.property ? object : getObjectValue(object, lookup.property);

    // string
    if (is('string', valueProperty)) {
      const valueResponse = responseMap[valueProperty];

      if (valueResponse !== undefined) {
        if (lookup.property) setObjectValue(mongoObject, lookup.property, valueResponse);
        else return valueResponse;
      }
    }
    // mongoDB ObjectId
    else if (mongodb.ObjectId.isValid(valueProperty)) {
      const valueResponse = responseMap[valueProperty?.toString()];

      if (valueResponse !== undefined) {
        if (lookup.property) setObjectValue(mongoObject, lookup.property, valueResponse);
        else return valueResponse;
      }
    }
    // object
    else if (is('object', valueProperty)) {
      const id = valueProperty?.id || valueProperty?._id;

      const valueResponse = responseMap[id?.toString()];

      const previous = copy(getObjectValue(mongoObject, lookup.property));

      if (lookup.override) {
        lookup.override.forEach(item => {
          setObjectValue(valueResponse, item, getObjectValue(previous, item));
        });
      }

      if (valueResponse !== undefined) {
        if (lookup.property) setObjectValue(mongoObject, lookup.property, valueResponse);
        else return valueResponse;
      }
    }
    // array
    else if (is('array', valueProperty)) {
      valueProperty.forEach((valuePropertyItem: any, index: number) => {
        const lookupItem = { ...lookup };

        lookupItem.property = `${lookupItem.property || ''}${lookupItem.property ? '.' : ''}${index}`;

        this.updateLookupProperty(mongoObject, valuePropertyItem, responseMap, lookupItem, true);
      });
    }
  }

  public getLookupIDs(value: any) {
    const ids = [];

    if (
      is('string', value) ||
      mongodb.ObjectId.isValid(value) ||
      is('array', value) ||
      is('object', value)
    ) {
      if (is('string', value)) ids.push(new mongodb.ObjectId(value));
      else if (mongodb.ObjectId.isValid(value)) ids.push(value);
      else if (is('object', value)) ids.push(new mongodb.ObjectId(value?.id || value?._id));
      else if (is('array', value)) ids.push(...value.flatMap(item => this.getLookupIDs(item)));
    }

    return ids;
  }

  protected toModel(value: any) {
    if (!this.Model || [null, undefined].includes(value)) return value;

    return is('array', value) ? value.map(item => new this.Model(item, false)) : new this.Model(value, false);
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

      arguments_.push(`Duration: ${duration(OnesyDate.utc.milliseconds - start, true)}`);

      this.onesyLog.debug(...arguments_);
    }

    if (value && this.Model !== undefined) {
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
    if (BaseCollection.isOnesyQuery(query)) {
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

  public static value(value: any) {
    // Getter object method
    if (is('function', value?.toObject)) return value.toObject();

    return { ...value };
  }

  public static isOnesyQuery(value: any) {
    return value instanceof Query || (value?.hasOwnProperty('query') && value?.hasOwnProperty('queries'));
  }

}

export default BaseCollection;
