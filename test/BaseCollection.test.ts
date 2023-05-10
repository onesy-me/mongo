/* tslint:disable: no-shadowed-variable */
import { assert } from '@amaui/test';
import * as mongodb from 'mongodb';

import * as AmauiUtils from '@amaui/utils';
import AmauiLog from '@amaui/log';
import { AmauiDate } from '@amaui/date';
import { Query } from '@amaui/models';

import { Mongo, BaseCollection } from '../src';

import Config from '../utils/js/config';

const options = {
  uri: Config.config.db.mongo.uri,
  name: Config.config.db.mongo.name,
};

group('BaseCollection', () => {
  let mongo: Mongo;
  const messages = [];

  pre(async () => {
    mongo = new Mongo(options);

    AmauiLog.options.log.enabled = false;

    mongo.subscription.subscribe(message => messages.push(message));
  });

  postTo(async () => await mongo.connection);

  post(async () => {
    await mongo.reset();

    AmauiLog.options.log.enabled = true;
  });

  group('ACollection', () => {
    let aCollection: BaseCollection;
    let newObject: any;

    pre(async () => {
      class ACollection extends BaseCollection {

        public constructor() {
          super('a', mongo);
        }

      }

      aCollection = new ACollection();

      // Add some fixture data
      const items = [];

      for (const [index, _] of new Array(7).entries()) {
        await AmauiUtils.wait(40);

        items.push({
          _id: new mongodb.ObjectId(),
          data: {
            a: {
              a: [4, { a: 4 }],
              i: index,
            },
          },
          api_meta: {
            // Milliseconds for purposes of testing only
            added_at: new AmauiDate().utc.milliseconds,
          },
        });
      }

      for (const [index, _] of new Array(7).entries()) {
        await AmauiUtils.wait(40);

        items.push({
          _id: new mongodb.ObjectId(),
          data: {
            a: {
              a: [4, { a: 40 }],
              i: index,
            },
          },
          api_meta: {
            // Milliseconds for purposes of testing only
            added_at: new AmauiDate().utc.milliseconds,
          },
        });
      }

      await aCollection.addMany(items, { add_date: false });
    });

    to('db', async () => {
      const db = await aCollection.db;

      assert(db.databaseName).eq(options.name);
      assert(db.collection).exist;
    });

    to('paginatedField', () => assert(aCollection.paginatedField).eq('api_meta.added_at'));

    to('paginatedAscending', () => assert(aCollection.paginatedAscending).eq(-1));

    to('paginatedAscending', () => assert(aCollection.projection).eql({
      _id: 1,
      meta: '$$ROOT.meta',
      data: '$$ROOT.data',
      namespace: '$$ROOT.namespace',
      api_meta: '$$ROOT.api_meta',
    }));

    to('collection', async () => {
      const collection = await aCollection.collection();

      assert(collection.dbName).eq('amaui-test');
      assert(collection.collectionName).eq('a');
    });

    to('transaction', async () => {
      newObject = { _id: new mongodb.ObjectId() };

      await aCollection.transaction(async session => await aCollection.addOne(newObject, { session }));

      try {
        await aCollection.transaction(async session => {
          await aCollection.addOne({ _id: new mongodb.ObjectId() }, { session });

          // To revert back the add above
          throw new Error('');
        });
      } catch (error) { }

      assert((await aCollection.aggregate()).length).eq(15);
    });

    to('count', async () => {
      assert(await aCollection.count()).eq(15);
      assert(await aCollection.count(new Query({ queries: { find: { a: { 'data.a.a': 4 } } } }))).eq(14);
      assert(await aCollection.count(new Query({ queries: { find: { a: { 'data.a.a.a': 4 } } } }))).eq(7);
      assert(await aCollection.count(new Query({ queries: { find: { a: { 'data.a.a.a': 40 } } } }))).eq(7);
      assert(await aCollection.count(new Query({ queries: { find: { a: { 'data.a.a.a': 41 } } } }))).eq(0);
    });

    to('exists', async () => {
      assert(await aCollection.exists([{ 'data.a.a': 4 }, { 'data.a.a': 14 }], '$or')).eq(true);
      assert(await aCollection.exists([{ 'data.a.a': 4 }, { 'data.a.a': 14 }])).eq(false);
      assert(await aCollection.exists([{ 'data.a.a.a': 40 }])).eq(true);
      assert(await aCollection.exists([{ 'data.a.a.a': 41 }])).eq(false);
    });

    to('find', async () => {
      const query = new Query({ queries: { find: { a: { 'data.a.a': 4 } } } });

      query.limit = 4;
      query.skip = 3;
      query.total = true;
      query.sort = { 'api_meta.added_at': 1 };

      const response = await aCollection.find(query);

      assert(response.response.length).eq(4);
      assert(response.response[0].data.a.i).eq(3);
      assert(response.skip).eq(3);
      assert(response.limit).eq(4);
      assert(response.total).eq(14);
    });

    to('findOne', async () => {
      assert(await aCollection.findOne(new Query({ queries: { find: { a: { 'data.a.a.a': 4 } } } }))).exist;
      assert(await aCollection.findOne(new Query({ queries: { find: { a: { 'data.a.a.a': 41 } } } }))).eq(null);
    });

    to('aggregate', async () => {
      const query = new Query({
        queries: {
          aggregate: {
            a: [
              { $match: { $and: [{ 'data.a.a': 4 }] } },

              { $sort: { 'api_meta.added_at': 1 } },

              { $skip: 3 },

              { $limit: 4 },
            ],
          },
        },
      });

      const response = await aCollection.aggregate(query);

      assert(response.length).eq(4);
      assert(response[0].data.a.a[0]).eq(4);
    });

    group('searchMany', () => {

      to('searchMany', async () => {
        const query = new Query({
          queries: {
            search: {
              a: [
                { 'data.a.a': 4 },
                { 'data.a.a.a': 4 },
              ],
            },
            api: { a: [{ 'data.a.i': { $gte: 3 } }] },
          },
          limit: 2,
          skip: 1,
          total: true,
          sort: { 'api_meta.added_at': 1 },
        });

        const response = await aCollection.searchMany(query);

        assert(response.response.length).eq(2);
        assert(response.response[0].data.a.a[0]).eq(4);
        assert(response.skip).eq(1);
        assert(response.limit).eq(2);
        assert(response.total).eq(4);
      });

      to('additional', async () => {
        const query = new Query({
          queries: {
            search: {
              a: [
                { 'data.a.a': 4 },
                { 'data.a.a.a': 4 },
              ],
            },
            api: { a: [{ 'data.a.i': { $gte: 3 } }] },
          },
          total: true,
          sort: { 'api_meta.added_at': 1 },
        });

        const response = await aCollection.searchMany(query, {
          pre: [{ $match: { 'data.a.a.a': 4 } }],
          prePagination: [{ $addFields: { 'data.a': 4 } }],
          post: [{ $addFields: { 'a': 4 } }],
        });

        assert(response.response.length).eq(4);
        assert(response.response[0].a).eq(4);
        assert(response.response[0].data.a).eq(4);
        assert(response.total).eq(4);
      });

      group('next', () => {
        let query: any;
        let response: any;

        to('hasNext', async () => {
          query = new Query({
            queries: {
              search: {
                a: [
                  { 'data.a.a': 4 },
                  { 'data.a.a.a': 4 },
                ],
              },
            },
            skip: 1,
            limit: 4,
            sort: { 'api_meta.added_at': 1 },
          });

          response = await aCollection.searchMany(query);

          assert(response.response.length).eq(4);
          assert(response.hasNext).eq(true);
          assert(response.response[0].data.a.a[0]).eq(4);
          assert(response.skip).eq(1);
          assert(response.limit).eq(4);
        });

        to('next', async () => {
          query.next = response.next;

          delete query.skip;
          delete query.limit;

          response = await aCollection.searchMany(new Query(query));

          assert(response.response.length).eq(2);
          assert(response.hasNext).eq(false);
          assert(response.response[0].data.a.i).eq(5);
        });

        to('no next', async () => {
          response = await aCollection.searchMany(new Query({
            queries: {
              search: {
                a: [
                  { 'data.a.a': 4 },
                  { 'data.a.a.a': 4 },
                ],
              },
            },
            skip: 4,
            limit: 4,
            sort: { 'api_meta.added_at': 1 },
          }));

          assert(response.response.length).eq(3);
          assert(response.hasNext).eq(false);
        });

      });

      group('previous', () => {
        let query: any;
        let response: any;

        to('hasPrevious', async () => {
          query = new Query({
            queries: {
              search: {
                a: [
                  { 'data.a.a': 4 },
                  { 'data.a.a.a': 4 },
                ],
              },
            },
            skip: 4,
            limit: 4,
            sort: { 'api_meta.added_at': 1 },
          });

          response = await aCollection.searchMany(query);

          assert(response.response.length).eq(3);
          assert(response.hasNext).eq(false);
          assert(response.hasPrevious).eq(true);
          assert(response.response[0].data.a.i).eq(4);
          assert(response.skip).eq(4);
          assert(response.limit).eq(4);
        });

        to('previous', async () => {
          query.previous = response.previous;

          delete query.skip;
          delete query.limit;

          response = await aCollection.searchMany(new Query(query));

          assert(response.response.length).eq(4);
          assert(response.response[0].data.a.a[0]).eq(4);
          assert(response.hasPrevious).eq(false);
        });

        to('no previous', async () => {
          response = await aCollection.searchMany(new Query({
            queries: {
              search: {
                a: [
                  { 'data.a.a': 4 },
                  { 'data.a.a.a': 4 },
                ],
              },
            },
            skip: 0,
            limit: 4,
            sort: { 'api_meta.added_at': 1 },
          }));

          assert(response.response.length).eq(4);
          assert(response.hasPrevious).eq(false);
        });

      });

    });

    to('searchOne', async () => {
      assert(await aCollection.searchOne(new Query({
        queries: {
          search: {
            a: [
              { 'data.a.a': 4 },
              { 'data.a.a.a': 4 },
            ],
          },
        },
      }))).exist;
      assert(await aCollection.searchOne(new Query({
        queries: {
          search: {
            a: [
              { 'data.a.a': 4 },
              { 'data.a.a.a': 41 },
            ],
          },
        },
      }))).eq(undefined);
    });

    to('updateOne', async () => {
      const response: any = await aCollection.updateOne(
        new Query({
          queries: {
            find: {
              a: {
                _id: newObject._id,
              },
            },
          },
        }),
        {
          'data.a.a': 4,
        }
      );

      assert(response.data.a.a).eq(4);
    });

    to('removeOne', async () => {
      await aCollection.removeOne(
        new Query({
          queries: {
            find: {
              a: {
                _id: newObject._id,
              },
            },
          },
        })
      );

      assert(await aCollection.findOne(new Query({ queries: { find: { a: { _id: newObject._id } } } }))).eq(null);
    });

    to('updateOneOrAdd', async () => {
      newObject = await aCollection.updateOneOrAdd(
        new Query({
          queries: {
            find: {
              a: {
                'data.a.a': 40,
              },
            },
          },
        }),
        {
          'data.a.a': 40,
        }
      );

      assert(newObject._id).exist;
      assert(newObject.data.a.a).eq(40);
    });

    to('addOne', async () => {
      const response = await aCollection.addOne(
        {
          a: 440,
        }
      );

      assert(response._id).exist;
      assert(response.a).eq(440);
      assert(await aCollection.findOne(new Query({ queries: { find: { a: { a: 440 } } } }))).exist;
    });

    to('addMany', async () => {
      const response = await aCollection.addMany([
        { a: 4440 },
        { a: 4440 },
        { a: 4440 },
        { a: 4440 },
      ]);

      assert(response.length).eq(4);
      assert(response[0]._id).exist;
      assert(response[0].a).eq(4440);
      assert((await aCollection.aggregate(new Query({ queries: { aggregate: { a: [{ $match: { a: 4440 } }] } } }))).length).eq(4);
    });

    to('updateMany', async () => {
      const response = await aCollection.updateMany(
        new Query({
          queries: {
            find: {
              a: {
                a: 4440,
              },
            },
          },
        }),
        {
          'data.a.a': 4,
        },
        {
          $inc: {
            'a': 4,
          },
        }
      );

      const items = await aCollection.aggregate(new Query({ queries: { aggregate: { a: [{ $match: { a: 4444 } }] } } }));

      assert(response).eq(4);
      assert(items.length).eq(4);
      assert(items[0].data.a.a).eq(4);
    });

    to('removeMany', async () => {
      const response = await aCollection.removeMany(
        new Query({
          queries: {
            find: {
              a: {
                a: 4444,
              },
            },
          },
        })
      );

      const items = await aCollection.aggregate(new Query({ queries: { aggregate: { a: [{ $match: { a: 4444 } }] } } }));

      assert(response).eq(4);
      assert(items.length).eq(0);
    });

  });

});
