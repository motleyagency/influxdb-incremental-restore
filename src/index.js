#!/usr/bin/env node
'use strict';
/* eslint-disable no-console */
var __awaiter =
  (this && this.__awaiter) ||
  function(thisArg, _arguments, P, generator) {
    function adopt(value) {
      return value instanceof P
        ? value
        : new P(function(resolve) {
            resolve(value);
          });
    }
    return new (P || (P = Promise))(function(resolve, reject) {
      function fulfilled(value) {
        try {
          step(generator.next(value));
        } catch (e) {
          reject(e);
        }
      }
      function rejected(value) {
        try {
          step(generator['throw'](value));
        } catch (e) {
          reject(e);
        }
      }
      function step(result) {
        result.done
          ? resolve(result.value)
          : adopt(result.value).then(fulfilled, rejected);
      }
      step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
  };
var __generator =
  (this && this.__generator) ||
  function(thisArg, body) {
    var _ = {
        label: 0,
        sent: function() {
          if (t[0] & 1) throw t[1];
          return t[1];
        },
        trys: [],
        ops: [],
      },
      f,
      y,
      t,
      g;
    return (
      (g = { next: verb(0), throw: verb(1), return: verb(2) }),
      typeof Symbol === 'function' &&
        (g[Symbol.iterator] = function() {
          return this;
        }),
      g
    );
    function verb(n) {
      return function(v) {
        return step([n, v]);
      };
    }
    function step(op) {
      if (f) throw new TypeError('Generator is already executing.');
      while (_)
        try {
          if (
            ((f = 1),
            y &&
              (t =
                op[0] & 2
                  ? y['return']
                  : op[0]
                  ? y['throw'] || ((t = y['return']) && t.call(y), 0)
                  : y.next) &&
              !(t = t.call(y, op[1])).done)
          )
            return t;
          if (((y = 0), t)) op = [op[0] & 2, t.value];
          switch (op[0]) {
            case 0:
            case 1:
              t = op;
              break;
            case 4:
              _.label++;
              return { value: op[1], done: false };
            case 5:
              _.label++;
              y = op[1];
              op = [0];
              continue;
            case 7:
              op = _.ops.pop();
              _.trys.pop();
              continue;
            default:
              if (
                !((t = _.trys), (t = t.length > 0 && t[t.length - 1])) &&
                (op[0] === 6 || op[0] === 2)
              ) {
                _ = 0;
                continue;
              }
              if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) {
                _.label = op[1];
                break;
              }
              if (op[0] === 6 && _.label < t[1]) {
                _.label = t[1];
                t = op;
                break;
              }
              if (t && _.label < t[2]) {
                _.label = t[2];
                _.ops.push(op);
                break;
              }
              if (t[2]) _.ops.pop();
              _.trys.pop();
              continue;
          }
          op = body.call(thisArg, _);
        } catch (e) {
          op = [6, e];
          y = 0;
        } finally {
          f = t = 0;
        }
      if (op[0] & 5) throw op[1];
      return { value: op[0] ? op[1] : void 0, done: true };
    }
  };
var __spreadArrays =
  (this && this.__spreadArrays) ||
  function() {
    for (var s = 0, i = 0, il = arguments.length; i < il; i++)
      s += arguments[i].length;
    for (var r = Array(s), k = 0, i = 0; i < il; i++)
      for (var a = arguments[i], j = 0, jl = a.length; j < jl; j++, k++)
        r[k] = a[j];
    return r;
  };
exports.__esModule = true;
var path_1 = require('path');
var fs_1 = require('fs');
var fs_extra_1 = require('fs-extra');
var util_1 = require('util');
var lodash_flatten_1 = require('lodash.flatten');
var lodash_pick_1 = require('lodash.pick');
var lodash_groupby_1 = require('lodash.groupby');
var tmp_promise_1 = require('tmp-promise');
var meow_1 = require('meow');
var execa_1 = require('execa');
var p_limit_1 = require('p-limit');
var p_retry_1 = require('p-retry');
var copy = fs_extra_1['default'].copy,
  ensureDir = fs_extra_1['default'].ensureDir;
var readdir = util_1.promisify(fs_1['default'].readdir);
var cli = meow_1['default'](
  "\n  Usage\n  $ influxdb-incremental-restore <options> <path-to-backups>\n\n  Options\n    [ -host <host> ]: Host and port for InfluxDB OSS instance. Default value is '127.0.0.1'. Required for remote connections. Example: -host 127.0.0.1\n    [ -port <port> ]: Host and port for InfluxDB OSS instance. Default value is '8088'. Required for restore/backup connections. Example: -port 8088\n    [ -portHttp <port> ]: Host and port for InfluxDB OSS instance. Default value is '8086'. Required for Api connections. Example: -port 8086\n    [ -db <db_name>]: Name of the database to be restored from the backup. Required.\n    [ -newdb <newdb_name> ]: Name of the database into which the archived data will be imported on the target system. If not specified, then the value for -db is used. The new database name must be unique to the target system.\n    [ -rp <rp_name> ]: Name of the retention policy from the backup that will be restored. Requires that -db is set. If not specified, all retention policies will be used.\n    [ -newrp <newrp_name> ]: Name of the retention policy to be created on the target system. Requires that -rp is set. If not specified, then the -rp value is used.\n    [ -shard <shard_ID> ]: Shard ID of the shard to be restored. If specified, then -db and -rp are required.\n    [ -password <password> ]: Password to connect to the server.\n    [ -username <username> ]: Username to connect to the server.\n    [ -ssl ]: Use https for requests.\n    [ -unsafeSsl ]: Set this when connecting to the cluster using https and not use SSL verification.\n    [ -pps ] How many points per second the import will allow. By default it is zero and will not throttle importing.\n[ -useTargetMeasurements] Use measurements from target database, use if you get errors like '... input field \"<field>\" on measurement \"<measurement>\" is type float, already exists as type integer... '\n    [ -concurrency <number> ]: Amount of concurrent requests to the database. Default is 1.\n    [ --version ]: Display version and exit\n    [ --help ]: Display this help\n\n  Examples\n    $ influxdb-incremental-restore -db old-database ./backups\n    $ influxdb-incremental-restore -db old-database ./backups # restores old-database\n    $ influxdb-incremental-restore -db old-database -newdb new-database # restores old-database as new-database\n    $ influxdb-incremental-restore -db old-database -useTargetMeasurements\n    $ influxdb-incremental-restore --version\n    $ influxdb-incremental-restore --help\n",
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
    argv: process.argv.slice(2).map(function(i) {
      return /^-{1}((?!-).)*$/.test(i) ? '-' + i : i;
    }),
  },
);
var dumpFolder = cli.input[0],
  flags = cli.flags;
var CONCURRENCY = parseInt(flags.concurrency, 10) || 1;
if (!dumpFolder || dumpFolder.length === 0) {
  console.error('  ERROR: You must provide path to backup folder');
  console.log(cli.help);
  process.exit(1);
}
var useTargetMeasurements = flags.useTargetMeasurements;
if (!flags.db) {
  console.error(
    '  ERROR: You must provide database name to restore with --dbname',
  );
  process.exit(1);
}
var createHostPort = function(_a) {
  var isCombined = _a.isCombined,
    restore = _a.restore;
  var _b = flags.host,
    host = _b === void 0 ? '127.0.0.1' : _b,
    _c = flags.port,
    port = _c === void 0 ? '8088' : _c,
    _d = flags.portHttp,
    portHttp = _d === void 0 ? '8086' : _d;
  var p = restore ? port : portHttp;
  if (isCombined) {
    return ['-host', host + ':' + p];
  }
  return ['-host', host, '-port', p];
};
var createConfigFromFlags = function(values) {
  return lodash_flatten_1['default'](
    Object.entries(lodash_pick_1['default'](flags, values)).reduce(function(
      acc,
      _a,
    ) {
      var key = _a[0],
        val = _a[1];
      if (typeof val === 'boolean' && val === true) {
        acc.push(['-' + key]);
      } else if (typeof val === 'string') {
        acc.push(['-' + key, val]);
      }
      return acc;
    },
    []),
  );
};
var validateGroups = function(groups) {
  Object.entries(groups).forEach(function(_a) {
    var key = _a[0],
      entry = _a[1];
    var extensions = entry.map(function(i) {
      return i.split('.').pop();
    });
    var hasManifest = extensions.find(function(i) {
      return i === 'manifest';
    });
    var hasMeta = extensions.find(function(i) {
      return i === 'meta';
    });
    var hasShards = extensions.find(function(i) {
      return i === 'gz';
    });
    if (!hasManifest) {
      throw new Error('  ERROR: Missing manifest file for ' + key);
    }
    if (!hasMeta) {
      throw new Error('  ERROR: Missing meta file for ' + key);
    }
    if (!hasShards) {
      console.log('  WARN: ' + key + ' did not have anything to restore');
    }
  });
};
var config = { isCombined: false, restore: false };
var executeCommand = function(command, format) {
  if (format === void 0) {
    format = 'column';
  }
  return __awaiter(void 0, void 0, void 0, function() {
    var stdout;
    return __generator(this, function(_a) {
      switch (_a.label) {
        case 0:
          return [
            4 /*yield*/,
            execa_1['default'](
              'influx',
              __spreadArrays(
                createHostPort(config),
                createConfigFromFlags([
                  'password',
                  'username',
                  'ssl',
                  'unsafeSsl',
                  'pps',
                ]),
                ['-format=' + format, '-execute', command],
              ),
            ),
          ];
        case 1:
          stdout = _a.sent().stdout;
          return [2 /*return*/, stdout];
      }
    });
  });
};
var parseResults = function(res) {
  return res.results.reduce(function(acc, i) {
    i.series.forEach(function(k) {
      acc[k.name] = k.values.map(function(_a) {
        var key = _a[0],
          value = _a[1];
        return { key: key, value: value };
      });
    });
    return acc;
  }, {});
};
var parseTags = function(tags) {
  return tags.map(function(item) {
    return item.key + '::tag';
  });
};
var parseFields = function(fields, tags) {
  return fields
    .filter(function(field) {
      return !tags.find(function(tag) {
        return tag.key === field.key;
      });
    })
    .map(function(field) {
      return field.key + '::' + field.value;
    });
};
var runMergeScript = function(groups) {
  return __awaiter(void 0, void 0, void 0, function() {
    var limit,
      keys,
      targetDatabase,
      measurements,
      _a,
      _b,
      _c,
      _d,
      tagKeys,
      _e,
      _f,
      _g;
    return __generator(this, function(_h) {
      switch (_h.label) {
        case 0:
          limit = p_limit_1['default'](CONCURRENCY);
          keys = Object.keys(groups);
          return [
            4 /*yield*/,
            executeCommand('CREATE DATABASE ' + (flags.newdb || flags.db)),
          ];
        case 1:
          _h.sent();
          targetDatabase = '' + (flags.newdb || flags.db);
          if (!useTargetMeasurements) return [3 /*break*/, 3];
          _b = parseResults;
          _d = (_c = JSON).parse;
          return [
            4 /*yield*/,
            executeCommand('SHOW field keys ON ' + targetDatabase, 'json'),
          ];
        case 2:
          _a = _b.apply(void 0, [_d.apply(_c, [_h.sent()])]);
          return [3 /*break*/, 4];
        case 3:
          _a = { '*': [] };
          _h.label = 4;
        case 4:
          measurements = _a;
          _e = parseResults;
          _g = (_f = JSON).parse;
          return [
            4 /*yield*/,
            executeCommand('SHOW tag keys ON ' + targetDatabase, 'json'),
          ];
        case 5:
          tagKeys = _e.apply(void 0, [_g.apply(_f, [_h.sent()])]) || {
            '*': [],
          };
          return [
            2 /*return*/,
            Promise.all(
              keys.map(function(key) {
                var tempDatabase = flags.db + '_' + key;
                return Promise.all(
                  Object.entries(measurements).map(function(_a) {
                    var measurement = _a[0],
                      values = _a[1];
                    var run = function() {
                      return __awaiter(void 0, void 0, void 0, function() {
                        var tag, fieldKeys;
                        return __generator(this, function(_a) {
                          switch (_a.label) {
                            case 0:
                              if (!useTargetMeasurements)
                                return [3 /*break*/, 2];
                              tag = parseTags(tagKeys[measurement]);
                              fieldKeys = parseFields(
                                values,
                                tagKeys[measurement],
                              );
                              console.log(
                                'Using target measurements:  ' +
                                  tag.join(',') +
                                  ',' +
                                  fieldKeys.join(','),
                              );
                              return [
                                4 /*yield*/,
                                executeCommand(
                                  'SELECT ' +
                                    tag.join(',') +
                                    ',' +
                                    fieldKeys.join(',') +
                                    ' INTO ' +
                                    targetDatabase +
                                    '..' +
                                    measurement +
                                    ' FROM  ' +
                                    tempDatabase +
                                    '..' +
                                    measurement +
                                    ' GROUP BY *',
                                ),
                              ];
                            case 1:
                              _a.sent();
                              return [3 /*break*/, 4];
                            case 2:
                              console.log(
                                'Using wild card for copying measurements',
                              );
                              return [
                                4 /*yield*/,
                                executeCommand(
                                  'SELECT * INTO ' +
                                    targetDatabase +
                                    '..:MEASUREMENT FROM  ' +
                                    tempDatabase +
                                    '../.*/ GROUP BY *',
                                ),
                              ];
                            case 3:
                              _a.sent();
                              _a.label = 4;
                            case 4:
                              console.info(
                                '  INFO: Merged ' +
                                  (measurement || '*') +
                                  ' ON ' +
                                  tempDatabase +
                                  ' to ' +
                                  targetDatabase +
                                  '!',
                              );
                              return [2 /*return*/, Promise.resolve()];
                          }
                        });
                      });
                    };
                    return limit(function() {
                      return p_retry_1['default'](run, {
                        onFailedAttempt: function(error) {
                          console.log(
                            'Attempt ' +
                              error.attemptNumber +
                              ' for ' +
                              tempDatabase +
                              ' to ' +
                              targetDatabase +
                              ' failed. There are ' +
                              error.attemptsLeft +
                              ' attempts left.\n',
                            '' +
                              (error.stderr &&
                              error.stderr.includes('type conflict')
                                ? '\nType conflict: Consider using -useTargetMeasurements flag\n\n'
                                : ''),
                          );
                        },
                        retries: 5,
                      });
                    });
                  }),
                ).then(function() {
                  return executeCommand('DROP DATABASE ' + tempDatabase);
                });
              }),
            ),
          ];
      }
    });
  });
};
var restoreGroups = function(groups) {
  return __awaiter(void 0, void 0, void 0, function() {
    var tmpDir, limit, err_1;
    return __generator(this, function(_a) {
      switch (_a.label) {
        case 0:
          return [
            4 /*yield*/,
            tmp_promise_1['default'].dir({ unsafeCleanup: true }),
          ];
        case 1:
          tmpDir = _a.sent();
          limit = p_limit_1['default'](CONCURRENCY);
          _a.label = 2;
        case 2:
          _a.trys.push([2, 4, , 5]);
          return [
            4 /*yield*/,
            Promise.all(
              Object.entries(groups).map(function(_a) {
                var key = _a[0],
                  entry = _a[1];
                return __awaiter(void 0, void 0, void 0, function() {
                  var tmpPath;
                  return __generator(this, function(_b) {
                    switch (_b.label) {
                      case 0:
                        tmpPath = path_1['default'].resolve(tmpDir.path, key);
                        return [4 /*yield*/, ensureDir(tmpPath)];
                      case 1:
                        _b.sent();
                        return [
                          4 /*yield*/,
                          Promise.all(
                            entry.map(function(i) {
                              return copy(
                                path_1['default'].resolve(
                                  process.cwd(),
                                  dumpFolder,
                                  i,
                                ),
                                path_1['default'].resolve(tmpPath, i),
                              );
                            }),
                          ),
                        ];
                      case 2:
                        _b.sent();
                        return [
                          2 /*return*/,
                          limit(function() {
                            return __awaiter(
                              void 0,
                              void 0,
                              void 0,
                              function() {
                                var shouldRun, data, err_2, result;
                                return __generator(this, function(_a) {
                                  switch (_a.label) {
                                    case 0:
                                      console.info(
                                        '  INFO: Restoring ' +
                                          flags.db +
                                          '_' +
                                          key +
                                          '...',
                                      );
                                      shouldRun = true;
                                      _a.label = 1;
                                    case 1:
                                      _a.trys.push([1, 3, , 4]);
                                      return [
                                        4 /*yield*/,
                                        executeCommand(
                                          'SHOW MEASUREMENTS ON ' +
                                            flags.db +
                                            '_' +
                                            key,
                                          'column',
                                        ),
                                      ];
                                    case 2:
                                      data = _a.sent();
                                      if (!data) {
                                        console.log(
                                          'Skipping ' +
                                            flags.db +
                                            '_' +
                                            key +
                                            ' as it already exists',
                                        );
                                        shouldRun = false;
                                      }
                                      return [3 /*break*/, 4];
                                    case 3:
                                      err_2 = _a.sent();
                                      console.error(
                                        'Failed to execute command',
                                      );
                                      shouldRun = false;
                                      return [3 /*break*/, 4];
                                    case 4:
                                      if (!shouldRun) return [3 /*break*/, 6];
                                      return [
                                        4 /*yield*/,
                                        execa_1['default'](
                                          'influxd',
                                          __spreadArrays(
                                            ['restore', '-portable'],
                                            createHostPort({
                                              isCombined: true,
                                              restore: true,
                                            }),
                                            createConfigFromFlags([
                                              'db',
                                              'rp',
                                              'newrp',
                                              'shard',
                                            ]),
                                            [
                                              '-newdb',
                                              flags.db + '_' + key,
                                              tmpPath,
                                            ],
                                          ),
                                        ),
                                      ];
                                    case 5:
                                      result = _a.sent();
                                      console.log(result.stdout);
                                      console.log(result.stderr);
                                      console.info(
                                        '  Restored ' +
                                          flags.db +
                                          '_' +
                                          key +
                                          '!',
                                      );
                                      _a.label = 6;
                                    case 6:
                                      return [2 /*return*/, Promise.resolve()];
                                  }
                                });
                              },
                            );
                          }),
                        ];
                    }
                  });
                });
              }),
            ),
          ];
        case 3:
          _a.sent();
          return [3 /*break*/, 5];
        case 4:
          err_1 = _a.sent();
          tmpDir.cleanup();
          throw err_1;
        case 5:
          return [2 /*return*/];
      }
    });
  });
};
(function() {
  return __awaiter(void 0, void 0, void 0, function() {
    var names, groups, err_3;
    return __generator(this, function(_a) {
      switch (_a.label) {
        case 0:
          _a.trys.push([0, 4, , 5]);
          return [4 /*yield*/, readdir(dumpFolder)];
        case 1:
          names = (_a.sent() || []).filter(function(i) {
            var ext = i.split('.').pop();
            return ['manifest', 'meta', 'gz'].some(function(x) {
              return x === ext;
            });
          });
          groups = lodash_groupby_1['default'](names, function(i) {
            return i.split('.')[0];
          });
          if (!groups[0] || groups[0].length === 0) {
            console.info('  INFO:  Nothing to restore, exiting...');
            process.exit(0);
          }
          validateGroups(groups);
          return [4 /*yield*/, restoreGroups(groups)];
        case 2:
          _a.sent();
          console.info('  INFO: Wrote measurements, merging databases...');
          return [4 /*yield*/, runMergeScript(groups)];
        case 3:
          _a.sent();
          console.log(
            '  Restored ' +
              Object.keys(groups).length +
              ' incremental backups successfully',
          );
          return [3 /*break*/, 5];
        case 4:
          err_3 = _a.sent();
          console.error('ERROR: ' + err_3.message + '}', err_3);
          process.exit(1);
          return [3 /*break*/, 5];
        case 5:
          return [2 /*return*/];
      }
    });
  });
})();
