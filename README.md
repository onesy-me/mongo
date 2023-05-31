
</br >
</br >

<p align='center'>
  <a target='_blank' rel='noopener noreferrer' href='#'>
    <img src='utils/images/logo.svg' alt='amaui logo' />
  </a>
</p>

<h1 align='center'>amaui Mongo</h1>

<p align='center'>
  Mongo
</p>

<br />

<h3 align='center'>
  <sub>MIT license&nbsp;&nbsp;&nbsp;&nbsp;</sub>
  <sub>Production ready&nbsp;&nbsp;&nbsp;&nbsp;</sub>
  <sub>100% test cov&nbsp;&nbsp;&nbsp;&nbsp;</sub>
  <sub>Nodejs</sub>
</h3>

<p align='center'>
    <sub>Very simple code&nbsp;&nbsp;&nbsp;&nbsp;</sub>
    <sub>Modern code&nbsp;&nbsp;&nbsp;&nbsp;</sub>
    <sub>Junior friendly&nbsp;&nbsp;&nbsp;&nbsp;</sub>
    <sub>Typescript&nbsp;&nbsp;&nbsp;&nbsp;</sub>
    <sub>Made with :yellow_heart:</sub>
</p>

<br />

## Getting started

### Add

```sh
  yarn add @amaui/mongo
```

### Use

```javascript
  import { Mongo, BaseCollection } from '@amaui/mongo';
  import { Query } from '@amaui/models';
  // Make if you wanna a config file and
  // inside of it add all the process.env related props
  import Config from './config';

  // Make a new mongo instance
  const mongo = new Mongo({
    uri: Config.db.mongo.uri,
    name: Config.db.mongo.name,
  });

  await mongo.connection;

  // Make a collection class
  class ACollection extends BaseCollection {

    public constructor() {
      super('a', mongo);
    }

  }

  const aCollection = new ACollection();

  // Add
  const value = await aCollection.addOne({ _id: new ObjectId(), a: 'a' });

  // Find one
  const item = await aCollection.findOne(new Query({
    queries: {
      find: { a: value._id },
    },
  }));

  // { _id: ObjectId('407f191e810c19729de860ea'), a: 'a' }

  // etc.
```

### Dev

#### One time local setup

Install docker and docker-compose

  - https://docs.docker.com/get-docker
  - https://docs.docker.com/compose/install

Make docker containers

```sh
  yarn docker
```

Make mongoDB replica set

```sh
  yarn make-replicaset
```

(mac) Add lines below to */private/etc/hosts*
```
127.0.0.1   mongo1
127.0.0.1   mongo2
127.0.0.1   mongo3
127.0.0.1   mongo1-test
127.0.0.1   mongo2-test
127.0.0.1   mongo3-test
```

Install

```sh
  yarn
```

Test

```sh
  yarn test
```

### Prod

Build

```sh
  yarn build
```
