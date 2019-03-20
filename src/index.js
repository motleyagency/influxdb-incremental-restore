#!/usr/bin/env node
// @flow
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

type Groups = {
  [string]: Array<string>,
};

type Flags = {
  host?: string,
  port?: string,
  portHttp?: string,
  db: string,
  newdb?: string,
  rp?: string,
  newrp?: string,
  shard?: string,
  password?: string,
  username?: string,
  ssl?: string,
  unsafeSsl: string,
  pps?: string,
  concurrency?: string,
  useTargetMeasurements?: boolean,
};

type Cli = {
  flags: Flags,
  input: [string],
  help: string,
};

type HostPortConfig = {
  isCombined: boolean,
  restore: boolean,
};

type InfluxDataResult = {
  stdout: string,
};

const cli: Cli = meow(
  `
  Usage
  $ influxdb-incremental-restore <options> <path-to-backups>

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
    [ -useTargetMeasurements] Use measrements from target database, use if you get error like '... input field "<field>" on measurement "<measurement>" is type float, already exists as type integer... '
    [ -concurrency <number> ]: Amount of concurrent requests to the database. Default is 1.
    [ --version ]: Display version and exit
    [ --help ]: Display this help

  Examples
    $ influxdb-incremental-restore -db old-database ./backups
    $ influxdb-incremental-restore -db old-database ./backups # restores old-database
    $ influxdb-incremental-restore -db old-database -newdb new-database # restores old-database as new-database
    $ influxdb-incremental-restore -db old-database -useTargetMeasurements
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
    portHttp: {
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
    useTargetMeasurements: {
      type: 'boolean',
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

const { useTargetMeasurements } = flags;

if (!flags.db) {
  console.error(
    '  ERROR: You must provide database name to restore with --dbname',
  );
  process.exit(1);
}

const createHostPort = ({ isCombined, restore }: HostPortConfig): string[] => {
  const { host = '127.0.0.1', port = '8088', portHttp = '8086' } = flags;
  const p = restore ? port : portHttp;

  if (isCombined) {
    return ['-host', `${host}:${p}`];
  }

  return ['-host', host, `-port`, p];
};

const createConfigFromFlags = (values: string[]): string[] =>
  flatten(
    Object.entries(pick(flags, values)).reduce(
      (acc: Array<string[]>, [key, val]): Array<Array<string>> => {
        if (typeof val === 'boolean' && val === true) {
          acc.push([`-${key}`]);
        } else if (typeof val === 'string') {
          acc.push([`-${key}`, val]);
        }
        return acc;
      },
      [],
    ),
  );

const validateGroups = (groups: Groups) => {
  Object.entries(groups).forEach(([key, entry]) => {
    // $FlowFixMe
    const extensions: string[] = (entry: string[]).map(i => i.split('.').pop());
    const hasManifest: string | void = extensions.find(i => i === 'manifest');
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
const config: HostPortConfig = { isCombined: false, restore: false };

const executeCommand = (
  command: string,
  format: string = 'column',
): Promise<InfluxDataResult> =>
  execa('influx', [
    ...createHostPort(config),
    ...createConfigFromFlags([
      'password',
      'username',
      'ssl',
      'unsafeSsl',
      'pps',
    ]),
    `-format=${format}`,
    '-execute',
    command,
  ]);

const parseResults = res =>
  res.results.reduce((acc, i) => {
    i.series.forEach(k => {
      acc[k.name] = k.values.map(([key, value]) => ({ key, value }));
    });
    return acc;
  }, {});

const parseTags = (tags): Array<string> => tags.map(item => `${item.key}::tag`);

const parseFields = (fields: Array<Object>, tags): string[] =>
  fields
    .filter(field => !tags.find(tag => tag.key === field.key))
    .map(field => `${field.key}::${field.value}`);

const runMergeScript = async (groups: Groups): Promise<InfluxDataResult[]> => {
  // $FlowFixMe
  const limit = pLimit(CONCURRENCY);

  const keys = Object.keys(groups);

  await executeCommand(`CREATE DATABASE ${flags.newdb || flags.db}`);
  const defaultResult: InfluxDataResult = { stdout: '*' };
  const targetDatabase = `${flags.newdb || flags.db}`;
  const measurementsFields = useTargetMeasurements
    ? [
        executeCommand(`SHOW field keys ON ${targetDatabase}`, 'json'),
        executeCommand(`SHOW tag keys ON ${targetDatabase}`, 'json'),
      ]
    : [Promise.resolve(defaultResult)];

  return Promise.all(measurementsFields).then(([fields, tags]) => {
    const measurements = useTargetMeasurements
      ? parseResults(JSON.parse(fields.stdout))
      : { '*': [] };
    const tagKeys = useTargetMeasurements
      ? parseResults(JSON.parse(tags.stdout))
      : { '*': [] };

    return Promise.all(
      keys.map(key => {
        const tempDatabase = `${flags.db}_${key}`;
        return Promise.all(
          Object.entries(measurements).map(([measurement, values]) => {
            const run = async () => {
              if (useTargetMeasurements) {
                const tag = parseTags(tagKeys[measurement]);
                const fieldKeys = parseFields(values, tagKeys[measurement]);
                console.log(
                  `Using target measurements:  ${tag.join(
                    ',',
                  )},${fieldKeys.join(',')}`,
                );
                await executeCommand(
                  `SELECT ${tag.join(',')},${fieldKeys.join(
                    ',',
                  )} INTO ${targetDatabase}..${measurement} FROM  ${tempDatabase}..${measurement} GROUP BY *`,
                );
              } else {
                console.log('Using wild card for copying measurements');
                await executeCommand(
                  `SELECT * INTO ${targetDatabase}..:MEASUREMENT FROM  ${tempDatabase}../.*/ GROUP BY *`,
                );
              }

              console.info(
                `  INFO: Merged ${measurement ||
                  `*`} ON ${tempDatabase} to ${targetDatabase}!`,
              );
              return Promise.resolve();
            };
            return limit(() =>
              pRetry(run, {
                onFailedAttempt: error => {
                  console.log(
                    `Attempt ${
                      error.attemptNumber
                    } for ${tempDatabase} to ${targetDatabase} failed. There are ${
                      error.attemptsLeft
                    } attempts left.\n`,
                    `${
                      error.stderr && error.stderr.indexOf('type conflict') >= 0
                        ? `\nType conflict: Consider using -useTargetMeasurements flag\n\n`
                        : ``
                    }`,
                  );
                },
                retries: 5,
              }),
            );
          }),
        ).then(() => executeCommand(`DROP DATABASE ${tempDatabase}`));
      }),
    );
  });
};

const restoreGroups = async (groups: Groups): Promise<InfluxDataResult> => {
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

        return limit(() => {
          console.info(`  INFO: Restoring ${flags.db}_${key}...`);
          return executeCommand(
            `SHOW MEASUREMENTS ON ${flags.db}_${key}`,
            'column',
          ).then(({ stdout, failed }) => {
            // no results found or failed

            if (!stdout || failed) {
              return execa('influxd', [
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
                console.info(`  Restored ${flags.db}_${key}!`);
              });
            }
            console.log(`Skipping ${flags.db}_${key} as it already exists`);
          });
        });
      }),
    );
  } catch (err) {
    tmpDir.cleanup();
    throw err;
  }
};

(async () => {
  try {
    const names: string[] = ((await readdir(dumpFolder)) || []).filter(
      (i: string): boolean => {
        const ext: string = i.split('.').pop();
        return ['manifest', 'meta', 'gz'].some(x => x === ext);
      },
    );
    const groups: Groups = groupBy(names, i => i.split('.')[0]);

    if (groups.length === 0) {
      console.info('  INFO:  Nothing to restore, exiting...');
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
