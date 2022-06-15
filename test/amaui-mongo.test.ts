/* tslint:disable: no-shadowed-variable */
import { assert } from '@amaui/test';

import * as AmauiUtils from '@amaui/utils';

import { AmauiMongo, Mongo } from '../src';

group('@amaui/mongo/amaui-mongo', () => {

  group('AmauiMongo', () => {

    to('Mongo', () => {
      assert(AmauiMongo.mongo instanceof Mongo);
    });

    to('createPaginator', () => {
      const value = AmauiMongo.createPaginator({ api_meta: { added_at: 1441227440 } }, ['api_meta.added_at']);

      assert(value).eq('eyJhcGlfbWV0YS5hZGRlZF9hdCI6eyIkZ3QiOjE0NDEyMjc0NDB9fQ==');
      assert(AmauiUtils.deserialize(AmauiUtils.decode(value))).eql({ 'api_meta.added_at': { $gt: 1441227440 } });
    });

  });

});
