const Debug = require('debug');
const path = require('path');
const pg = require('pg');
const config = require('typed-env-config');
const loader = require('taskcluster-lib-loader');
const taskcluster = require('taskcluster-client');
const {Handler} = require('./handler');
const api = require('./api');
const validator = require('taskcluster-lib-validate');
const App = require('taskcluster-lib-app');
const {Client, pulseCredentials} = require('taskcluster-lib-pulse');
const monitor = require('taskcluster-lib-monitor');

let debug = Debug('taskcluster-analysis:main');

let load = loader({
  cfg: {
    requires: ['profile'],
    setup: ({profile}) => config({profile}),
  },

  validator: {
    requires: ['cfg'],
    setup: ({cfg}) => validator({
      prefix: 'taskcluster-task-analysis/v1/',
      publish: false,
    }),
  },

  monitor: {
    requires: ['process', 'profile', 'cfg'],
    setup: async ({process, profile, cfg}) => await monitor({
      projectName: 'taskcluster-task-analysis',
      mock: true,
    }),
  },

  pulseClient: {
    requires: ['cfg', 'monitor'],
    setup: ({cfg, monitor}) => {
      return new Client({
        namespace: 'taskcluster-task-analysis',
        monitor: monitor.prefix('pulse-client'),
        credentials: pulseCredentials(cfg.pulse.credentials),
      });
    },
  },

  db: {
    requires: ['cfg'],
    setup: async ({cfg}) => {
      if (process.env.NODE_ENV === 'production') {
        debug('Running in production, forcing SSL for postgres');
        pg.defaults.ssl = true;
      }
      let client = new pg.Client(cfg.postgresql);
      await client.connect();
      return client;
    },
  },

  eventListener: {
    requires: ['cfg', 'pulseClient', 'db'],
    setup: async ({cfg, pulseClient, db}) => {
      let queue = new taskcluster.Queue({
        rootUrl: process.env.TASKCLUSTER_ROOT_URL,
      });

      let handler = new Handler({
        queue,
        pulseClient,
        taskQueueName: cfg.pulse.queueName,
        db,
      });
      handler.start();
    },
  },

  api: {
    requires: ['cfg', 'validator', 'db'],
    setup: ({cfg, validator, db}) => api.setup({
      context:          {cfg, db},
      publish:          false,
      baseUrl:          cfg.server.publicUrl + '/v1',
      referencePrefix:  '',
      validator,
    }),
  },

  server: {
    requires: ['cfg', 'api'],
    setup: ({cfg, api}) => {
      debug('Launching server.');
      let app = App(cfg.server);
      app.use('/v1', api);
      return app.createServer();
    },
  },

}, ['profile', 'process']);

// If this file is executed launch component from first argument
if (!module.parent) {
  load(process.argv[2], {
    profile: process.env.NODE_ENV,
    process: process.argv[2],
  }).catch(err => {
    console.log('Server crashed: ' + err.stack);
    process.exit(1);
  });
}

module.exports = load;
