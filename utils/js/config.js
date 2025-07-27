const path = require('path');
const dotenv = require('dotenv');

const envPath = path.resolve(__dirname, '../../', `.env.${process.env.NODE_ENV === 'test' ? 'test' : 'dev'}`);

dotenv.config({ path: envPath });

class Config {
  default = {
    db: {
      mongo: {},
    },
  };

  get config() {
    return {
      db: {
        mongo: {
          uri: process.env.ONESY_DB_MONGO_URI || this.default.db.mongo.uri,
          name: process.env.ONESY_DB_MONGO_NAME || this.default.db.mongo.name,
        },
      },
    };
  }
}

module.exports = new Config();
