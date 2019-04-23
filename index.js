var util = require('util');
var moment = require('moment');
var mssql = require('mssql');
var Base = require('db-migrate-base');
var Promise = require('bluebird');
var log;
var type;

var internals = {};

var MssqlDriver = Base.extend({
  init: function(connection) {
    this._super(internals);
    this.connection = connection;
  },

  addMigrationRecord: function(name, callback) {
    var formattedDate = moment(new Date()).format('YYYY-MM-DD HH:mm:ss');
    return this.runSql(`INSERT INTO [${internals.migrationTable}] ([name], [run_on]) VALUES ('${name}', '${formattedDate}')`).then(function(result) {
      return callback(false);
    }).catch(function(err) {
      console.log('err: ');
      console.log(err);
      return callback(true);
    });
  },

  runSql: function(sqlStatement) {
    var self = this;
    return new Promise(function(resolve, reject) {

      log.sql.apply(null, arguments);
      if(internals.dryRun) {
        console.log('THIS IS A DRY RUN');
        return resolve();
      }
      return self.connection.query(sqlStatement).then(function(queryResult) {
        return resolve((queryResult && queryResult.recordset) ? queryResult.recordset : []);
      }).catch(function(err) {
        return reject(err);
      });
    });
  },

  _makeParamArgs: function(args) {
    var params = Array.prototype.slice.call(args);
    var sql = params.shift();
    var callback = params.pop();

    if (params.length > 0 && Array.isArray(params[0])) {
      params = params[0];
    }
    return [sql, params, callback];
  },

  all: function(sql) {
    var self = this;
    return new Promise(function(resolve, reject) {
      return self.connection.query(sql).then(function(queryResult) {
        return resolve(queryResult.recordset ? queryResult.recordset : []);
      }).catch(function(err) {
        return reject(err);
      });
    });
  },

  /**
   * Queries the migrations table
   *
   * @param callback
   */
  allLoadedMigrations: function(callback) {
    var sql = `SELECT * FROM [${internals.migrationTable}] ORDER BY run_on DESC, name DESC`;
    return this.all(sql).then(function(result) {
      return callback((result.recordset) ? result.recordset : [], null);
    }).catch(function(err) {
      return callback(null, err); // figureout callback params
    });
  },

  /**
   * Deletes a migration
   *
   * @param migrationName   - The name of the migration to be deleted
   * @param callback
   */
  deleteMigration: function(migrationName, callback) {
    var sql = `DELETE FROM [${internals.migrationTable}] WHERE name = ${migrationName}`;
    this.runSql(sql).then(function(result) {
      return callback();
    }).catch(function(err) {
      console.log('err: ');
      console.log(err);
      return callback(err);
    });
  },

  createTable: function(tableName, options, callback) {
    log.verbose('creating table:', tableName);
    var columnSpecs = options,
        tableOptions = {};

    if (options.columns !== undefined) {
      columnSpecs = options.columns;
      delete options.columns;
      tableOptions = options;
    }

    var ifNotExistsSqlBegin = "";
    var ifNotExistsSqlEnd = "";
    if(tableOptions.ifNotExists) {
      ifNotExistsSql = `IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '${tableName}') BEGIN`;
      ifNotExistsSqlEnd = "END";
    }

    var primaryKeyColumns = [];
    var columnDefOptions = {
      emitPrimaryKey: false
    };

    for (var columnName in columnSpecs) {
      var columnSpec = this.normalizeColumnSpec(columnSpecs[columnName]);
      columnSpecs[columnName] = columnSpec;
      if (columnSpec.primaryKey) {
        primaryKeyColumns.push(columnName);
      }
    }

    var pkSql = '';
    if (primaryKeyColumns.length > 1) {
      pkSql = util.format(', PRIMARY KEY ([%s])', primaryKeyColumns.join('], ['));
    } else {
      columnDefOptions.emitPrimaryKey = true;
    }

    var columnDefs = [];
    var foreignKeys = [];

    for (var columnName in columnSpecs) {
      var columnSpec = columnSpecs[columnName];
      var constraint = this.createColumnDef(columnName, columnSpec, columnDefOptions, tableName);

      columnDefs.push(constraint.constraints);
      if (constraint.foreignKey)
        foreignKeys.push(constraint.foreignKey);
    }

    var sql = `${ifNotExistsSql} CREATE TABLE [${tableName}] (${columnDefs.join(', ')}${pkSql}) ${ifNotExistsSqlEnd}`;
    
    return this.runSql(sql).then(function(result) {
      return callback();
    }).catch(function(err) {
      console.log('err: ');
      console.log(err);
      return callback(err);
    });
  },

  close: function(callback) {
    return callback();
  },

  createColumnDef: function(name, spec, options, tableName) {
    var escapedName = util.format('[%s]', name),
        t = this.mapDataType(spec.type),
        len;
    if(spec.type !== type.TEXT && spec.type !== type.BLOB) {
      len = spec.length ? util.format('(%s)', spec.length) : '';
      if (t === 'VARCHAR' && len === '') {
        len = '(255)';
      }
    }
    var constraint = this.createColumnConstraint(spec, options, tableName, name);
    return { foreignKey: constraint.foreignKey,
             constraints: [escapedName, t, len, constraint.constraints].join(' ') };
  },

  createColumnConstraint: function(spec, options, tableName, columnName) {
    var constraint = [];
    var cb;

    if (spec.unsigned) {
      constraint.push('UNSIGNED');
    }

    if (spec.autoIncrement) {
      constraint.push('IDENTITY(1,1)');
    }

    if (spec.primaryKey) {
      if (!options || options.emitPrimaryKey) {
        constraint.push('PRIMARY KEY');
      }
    }

    if (spec.notNull === true) {
      constraint.push('NOT NULL');
    }

    if (spec.unique) {
      constraint.push('UNIQUE');
    }

    if (spec.engine && typeof(spec.engine) === 'string') {
      constraint.push('ENGINE=\'' + spec.engine + '\'')
    }

    if (spec.rowFormat && typeof(spec.rowFormat) === 'string') {
      constraint.push('ROW_FORMAT=\'' + spec.rowFormat + '\'')
    }

    if (spec.onUpdate && spec.onUpdate.startsWith('CURRENT_TIMESTAMP')) {
      constraint.push('ON UPDATE ' + spec.onUpdate)
    }

    if (spec.null || spec.notNull === false) {
      constraint.push('NULL');
    }

    if (spec.defaultValue !== undefined) {
      constraint.push('DEFAULT');

      if (typeof spec.defaultValue === 'string'){
        if(spec.defaultValue.startsWith('CURRENT_TIMESTAMP')) {
          constraint.push(spec.defaultValue);
        }
        else {
          constraint.push("'" + spec.defaultValue + "'");
        }
      } else if (spec.defaultValue === null) {
        constraint.push('NULL');
      } else {
        constraint.push(spec.defaultValue);
      }
    }

    if (spec.foreignKey) {

      cb = this.bindForeignKey(tableName, columnName, spec.foreignKey);
    }

    return { foreignKey: cb, constraints: constraint.join(' ') };
  },

  mapDataType: function(str) {
    switch (str) {
      case type.STRING:
        return 'VARCHAR';
      case type.TEXT:
        return 'TEXT';
      case type.INTEGER:
        return 'INTEGER';
      case type.BIG_INTEGER:
        return 'BIGINT';
      case type.DATE_TIME:
        return 'DATETIMEOFFSET';
      case type.REAL:
        return 'REAL';
      case type.BLOB:
        return 'BLOB';
      case type.TIMESTAMP:
        return 'TIMESTAMP';
      case type.BINARY:
        return 'BINARY';
      case type.BOOLEAN:
        return 'BOOLEAN';
      case type.DECIMAL:
        return 'DECIMAL';
      case type.CHAR:
        return 'CHAR';
      case type.DATE:
        return 'DATE';
      case type.SMALLINT:
        return 'SMALLINT';
      default:
        var unknownType = str.toUpperCase();
        log.warn('Using unknown data type', unknownType);
        return unknownType;
    }
  },

});

exports.connect = function(config, intern, callback) {
  internals = intern;
  log = internals.mod.log;
  type = internals.mod.type;

  return mssql.connect(config).then(function(arg1, arg2, arg3) {
    callback(null, new MssqlDriver(mssql));
  }).catch(function(err) {
    callback(err, null);
  });
  
};