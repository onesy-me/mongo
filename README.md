
</br>
</br>

<p align='center'>
  <a target='_blank' rel='noopener noreferrer' href='#'>
    <img width='auto' height='84' src='https://raw.githubusercontent.com/onesy-me/onesy/refs/heads/main/utils/images/logo.png' alt='onesy logo' />
  </a>
</p>

<h1 align='center'>onesy Mongo</h1>

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

### Add

```sh
yarn add @onesy/mongo
```

Add `mongodb` as a peer dependency

```sh
yarn add mongodb
```

### Use

```javascript
  import { Mongo, BaseCollection } from '@onesy/mongo';
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
  class TodoCollection extends BaseCollection {

    public constructor() {
      super('todos', mongo);
    }

  }

  const todoCollection = new TodoCollection();

  // Add
  const todoCreated = await todoCollection.addOne({
    name: 'todo',
    description: 'description'
  });

  // Find one
  const todo = await aCollection.findOne({
    _id: todoCreated._id
  });

  todo;

  // {
  //   _id: ObjectId('407f191e810c19729de860ea'),
  //   name: 'todo',
  //   description: 'description',
  //   added_at: 1777044477
  // }

  // etc.
```

### Dev

Install

```sh
yarn
```

Test

```sh
yarn test
```

#### One time local setup

Install docker and docker-compose

  - https://docs.docker.com/get-docker
  - https://docs.docker.com/compose/install

Make docker containers

```sh
yarn docker
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

### Prod

Build

```sh
yarn build
```
