#!/usr/bin/env node

/* eslint-disable no-console */
const path = require('path');
const fs = require('fs');
const { copy, ensureDir } = require('fs-extra');
const { promisify } = require('util');
const { flatten, groupBy, pick } = require('lodash');
const tmp = require('tmp-promise');
const meow = require('meow');
const execa = require('execa');
const pLimit = require('p-limit');
const pRetry = require('p-retry');

const readdir = promisify(fs.readdir);

const cli = meow(
  `
  Usage
  $ influx-incremental-restore <options> <path-to-backups>

  Options
    [ -host <host> ]: Host and port for InfluxDB OSS instance. Default value is '127.0.0.1'. Required for remote connections. Example: -host 127.0.0.1
    [ -port <port> ]: Host and port for InfluxDB OSS instance. Default value is '8088'. Required for restore/backup connections. Example: -port 8088
    [ -portHttp <port> ]: Host and port for InfluxDB OSS instance. Default value is '8086'. Required for Api connections. Example: -port 8086
    [ -db <db_name>]: Name of the database to be restored from the backup. Required.
    [ -newdb <newdb_name> ]: Name of the database into which the archived data will be imported on the target system. If not specified, then the value for -db is used. The new database name must be unique to the target system.
    [ -rp <rp_name> ]: Name of the retention policy from the backup that will be restored. Requires that -db is set. If not specified, all retention policies will be used.
    [ -newrp <newrp_name> ]: Name of the retention policy to be created on the target system. Requires that -rp is set. If not specified, then the -rp value is used.
    [ -shard <shard_ID> ]: Shard ID of the shard to be restored. If specified, then -db and -rp are required.
    [ -password <password> ]: Password to connect to the server.
    [ -username <username> ]: Username to connect to the server.
    [ -ssl ]: Use https for requests.
    [ -unsafeSsl ]: Set this when connecting to the cluster using https and not use SSL verification.
    [ -pps ] How many points per second the import will allow. By default it is zero and will not throttle importing.
    [ -concurrency <number> ]: Amount of concurrent requests to the database. Default is 1.
    [ --version ]: Display version and exit
    [ --help ]: Display this help

  Examples
    $ influx-incremental-restore -db old-database ./backups
    $ influxdb-incremental-restore -db old-database ./backups # restores old-database
    $ influxdb-incremental-restore -db old-database -newdb new-database # restores old-database as new-database
    $ influxdb-incremental-restore --version
    $ influxdb-incremental-restore --help
`,
  {
    host: {
      type: 'string',
    },
    port: {
      type: 'string',
    },
    httpPort: {
      type: 'string',
    },
    db: {
      type: 'string',
      alias: 'database',
    },
    newdb: {
      type: 'string',
    },
    rp: {
      type: 'string',
    },
    newrp: {
      type: 'string',
    },
    shard: {
      type: 'string',
    },
    password: {
      type: 'string',
    },
    username: {
      type: 'string',
    },
    ssl: {
      type: 'boolean',
    },
    unsafeSsl: {
      type: 'boolean',
    },
    pps: {
      type: 'string',
    },
    concurrency: {
      type: 'string',
    },
    description: 'CLI for incrementally restoring incremental InfluxDB backups',
    argv: process.argv
      .slice(2)
      .map(i => (/^-{1}((?!-).)*$/.test(i) ? `-${i}` : i)),
  },
);

const {
  input: [dumpFolder],
  flags,
} = cli;

const CONCURRENCY = parseInt(flags.concurrency, 10) || 1;

if (!dumpFolder || dumpFolder.length === 0) {
  console.error('  ERROR: You must provide path to backup folder');
  console.log(cli.help);
  process.exit(1);
}

if (!flags.db) {
  console.error(
    '  ERROR: You must provide database name to restore with --dbname',
  );
  process.exit(1);
}

const createHostPort = ({ isCombined, restore }) => {
  const { host = '127.0.0.1', port = '8088', portHttp = '8086' } = flags;
  const p = restore ? port : portHttp;

  if (isCombined) {
    return ['-host', `${host}:${p}`];
  }

  return ['-host', host, `-port`, p];
};

const createConfigFromFlags = values =>
  flatten(
    Object.entries(pick(flags, values)).reduce((acc, [key, val]) => {
      if (typeof val === 'boolean' && val === true) {
        acc.push([`-${key}`]);
      } else if (typeof val === 'string') {
        acc.push([`-${key}`, val]);
      }
      return acc;
    }, []),
  );

const validateGroups = groups => {
  Object.entries(groups).forEach(([key, entry]) => {
    // $FlowFixMe
    const extensions = entry.map(i => i.split('.').pop());
    const hasManifest = extensions.find(i => i === 'manifest');
    const hasMeta = extensions.find(i => i === 'meta');
    const hasShards = extensions.find(i => i === 'gz');

    if (!hasManifest) {
      throw new Error(`  ERROR: Missing manifest file for ${key}`);
    }

    if (!hasMeta) {
      throw new Error(`  ERROR: Missing meta file for ${key}`);
    }

    if (!hasShards) {
      console.log(`  WARN: ${key} did not have anything to restore`);
    }
  });
};

const runMergeScript = async groups => {
  // $FlowFixMe
  const limit = pLimit(CONCURRENCY);
  const config = { isCombined: false, restore: false };

  const executeCommand = command =>
    execa('influx', [
      ...createHostPort(config),
      ...createConfigFromFlags([
        'password',
        'username',
        'ssl',
        'unsafeSsl',
        'pps',
      ]),
      '-execute',
      command,
    ]);

  const keys = Object.keys(groups);

  await executeCommand(`CREATE DATABASE ${flags.newdb || flags.db}`);

  return Promise.all(
    keys.map(key => {
      const tempDatabase = `${flags.db}_${key}`;
      const targetDatabase = `${flags.newdb || flags.db}`;

      const run = async () => {
        await executeCommand(
          `SELECT * INTO ${targetDatabase}..:MEASUREMENT FROM ${tempDatabase}../.*/ GROUP BY *;`,
        );
        await executeCommand(`DROP DATABASE ${tempDatabase}`);

        return Promise.resolve();
      };

      return limit(() => pRetry(run, { retries: 5 }));
    }),
  );
};

const restoreGroups = async groups => {
  const tmpDir = await tmp.dir({ unsafeCleanup: true });
  const limit = pLimit(CONCURRENCY);

  try {
    await Promise.all(
      Object.entries(groups).map(async ([key, entry]) => {
        const tmpPath = path.resolve(tmpDir.path, key);
        await ensureDir(tmpPath);

        await Promise.all(
          // $FlowFixMe
          entry.map(i =>
            copy(
              path.resolve(process.cwd(), dumpFolder, i),
              path.resolve(tmpPath, i),
            ),
          ),
        );

        return limit(() =>
          execa('influxd', [
            'restore',
            '-portable',
            ...createHostPort({ isCombined: true, restore: true }),
            ...createConfigFromFlags(['db', 'rp', 'newrp', 'shard']),
            '-newdb',
            `${flags.db}_${key}`,
            tmpPath,
          ]).then(result => {
            console.log(result.stdout);
            console.log(result.stderr);
          }),
        );
      }),
    );
  } catch (err) {
    tmpDir.cleanup();
    throw err;
  }
};

(async () => {
  try {
    const names = ((await readdir(dumpFolder)) || []).filter(i => {
      const ext = i.split('.').pop();
      return ['manifest', 'meta', 'gz'].some(x => x === ext);
    });
    const groups = groupBy(names, i => i.split('.')[0]);

    if (groups.length === 0) {
      console.info('INFO:  Nothing to restore, exiting...');
      process.exit(0);
    }

    validateGroups(groups);
    await restoreGroups(groups);
    console.info('  INFO: Wrote measurements, merging databases...');
    await runMergeScript(groups);

    console.log(
      `  Restored ${
        Object.keys(groups).length
      } incremental backups successfully`,
    );
  } catch (err) {
    console.error(`ERROR: ${err.message}}`, err);
    process.exit(1);
  }
})();
