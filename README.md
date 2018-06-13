# InfluxDB incremental restore

> `influxdb-incremental-restore` InfluxDB incremental restore script with NodeJS

## Why

Currently there's [no way to incrementally restore](https://github.com/influxdata/influxdb/issues/9593#issuecomment-387693856)
incremental backups in InfluxDB (1.5.x). This script does just that for InfluxDB OSS instances.

### Prerequisites

* [NodeJS](https://nodejs.org/en/) > 8.9.0
* `influx` CLI tool (comes with InfluxDB)
* `influxd` CLI tool
* InfluxDB `-portable` backups (`meta`, `manifest` and `sX.tar.gz` (shards/data)) in a single level deep folder
* A running InfluxDB installation that does not contain a database with the given name.

#### Folder structure

```bash
backups/
├── <backup-prefix1>.meta
├── <backup-prefix1>.manifest
├── <backup-prefix1>.s1.tar.gz
├── <backup-prefix1>.s200.tar.gz
├── <backup-prefix1>.sX.tar.gz
├── <backup-prefix2>.meta
├── <backup-prefix2>.manifest
├── <backup-prefix2>.s1.tar.gz
├── <backup-prefix2>.s66.tar.gz
├── <backup-prefix2>.s199.tar.gz
└── <backup-prefix2>.sX.tar.gz
```

### How it works

The scripts runs in three different steps. First it creates a `tmp` folder and organises the data into groups according to their
prefixes. Next it uses `influxd` to restore the data to the database in the format of `${dbname}_${backup-prefix}`. If the restore
was successful, we create an empty database (by the name of `-db` value or `-newdb` if provided) and select all the measurements into
the database incrementally and drop the restored, post-fixed ones.

## Installation

```bash
npm install -g @motleyagency/influxdb-incremental-restore

// or

yarn global add @motleyagency/influxdb-incremental-restore
```

### Usage

```bash
CLI for incrementally restoring incremental InfluxDB backups

Usage
  $ influxdb-incremental-restore -db <db_name> <options> <path-to-backups>

Options

[ -host <host> ]: Host and port for InfluxDB OSS instance. Default value is '127.0.0.1'. Required for remote connections. Example: -host 127.0.0.1
[ -port <port> ]: Host and port for InfluxDB OSS instance. Default value is '8088'. Required for restore/backup connections. Example: -port 8088
[ -portHttp <port> ]: Host and port for InfluxDB OSS instance. Default value is '8086'. Required for Api connections. Example: -portHttp 8086
[ -db <db_name>]: Name of the database to be restored from the backup. Required.
[ -newdb <newdb_name> ]: Name of the database into which the archived data will be imported on the target system. If not specified, then the value for -db is used. The new database name must be unique to the target system.
[ -rp <rp_name> ]: Name of the retention policy from the backup that will be restored. Requires that -db is set. If not specified, all retention policies will be used.
[ -newrp <newrp_name> ]: Name of the retention policy to be created on the target system. Requires that -rp is set. If not specified, then the -rp value is used.
[ -shard <shard_ID> ]: Shard ID of the shard to be restored. If specified, then -db and -rp are required.
[ -password <password> ]: Password to connect to the server.
[ -username <username> ]: Username to connect to the server.
[ -ssl ]: Use https for requests.
[ -unsafeSsl ]: Set this when connecting to the cluster using https and not use SSL verification.
[ -pps ] How many points per second the import will allow.  By default it is zero and will not throttle importing.
[ -concurrency <number> ]: Amount of concurrent requests to the database. Default is 1.

General commands:
[ --version ]: Display version and exit
[ --help ]: Display this help

Examples
  $ influxdb-incremental-restore -db old-database ./backups # restores old-database
  $ influxdb-incremental-restore -db old-database -newdb new-database # restores old-database as new-database
  $ influxdb-incremental-restore --version
  $ influxdb-incremental-restore --help
```

> You can also use npx with `npx --package @motleyagency/influxdb-incremental-restore influx-incremental-restore <options> <path-to-backups>` if you don't want to install it globally.

### Versions

* `0.0.4`: Fix `portHttp` flag, few typo fixes
* `0.0.3`: Separate API and RPC port configurations
* `0.0.2`: Fix bin/-link.
* `0.0.1`: Initial release catered for our needs. Seems to work but YMMV.

### Contributions

Contributions are welcome! Send a PR or file a bug. Note that we follow a [Code Of Conduct](./CODE_OF_CONDUCT.md).

### License

MIT
