/* tslint:disable: no-shadowed-variable */
import { assert } from '@onesy/test';

import * as OnesyUtils from '@onesy/utils';

import { OnesyMongo, Mongo } from '../src';

group('OnesyMongo', () => {

  to('Mongo', () => {
    assert(OnesyMongo.mongo instanceof Mongo);
  });

  to('createPaginator', () => {
    const value = OnesyMongo.createPaginator({ api_meta: { added_at: 1441227440 } }, ['api_meta.added_at']);

    assert(value).eq('eyJhcGlfbWV0YS5hZGRlZF9hdCI6eyIkZ3QiOjE0NDEyMjc0NDB9fQ==');
    assert(OnesyUtils.deserialize(OnesyUtils.decode(value))).eql({ 'api_meta.added_at': { $gt: 1441227440 } });
  });

});
