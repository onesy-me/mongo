/* tslint:disable: no-shadowed-variable */
import { assert } from '@amaui/test';

import { Mongo } from '../src';

import Config from '../utils/js/config';

const options = {
  uri: Config.config.db.mongo.uri,
  name: Config.config.db.mongo.name,
};

group('@amaui/mongo/mongo', () => {
  let mongo: Mongo;
  const messages = [];

  pre(async () => {
    mongo = new Mongo(options);

    mongo.subscription.subscribe(message => messages.push(message));
  });

  post(async () => {
    await mongo.reset();
  });

  group('Mongo', () => {

    postTo(async () => await mongo.connection);

    to('connection', async () => {
      await mongo.connection;

      assert(mongo.connected).eq(true);
      assert(mongo.client).exist;
      assert(mongo.db.databaseName).eq(options.name);
      assert(messages.indexOf('connected') > -1).eq(true);
    });

    to('collections', async () => {
      await mongo.db.createCollection('ad');

      const collections = await mongo.getCollections(true);

      assert(collections.length).eq(1);
      assert(collections[0].name).eq('ad');
    });

    to('disconnect', async () => {
      await mongo.disconnect;

      assert(mongo.connected).eq(false);
      assert(mongo.db).eq(undefined);
      assert(mongo.client).eq(undefined);
      assert(messages.indexOf('disconnected') > -1).eq(true);
    });

    to('reset', async () => {
      await mongo.db.createCollection('a a a');

      await mongo.reset();

      const collections = await mongo.getCollections(true);

      assert(collections.length).eq(0);
    });

  });

});
