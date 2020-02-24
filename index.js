var util = require('util');
var mssql = require('mssql');
var Base = require('db-migrate-base');
var Promise = require('bluebird');

var MssqlDriver = Base.extend({
  init: function (connection, schema, intern) {
    this.log = intern.mod.log;
    this.type = intern.mod.type;
    this._escapeString = "'";
    this._super(intern);
    this.internals = intern;
    this.connection = connection;
    this.schema = schema || 'dbo';
  },

  createColumnDef: function (name, spec, options, tableName) {
    var type =
      spec.primaryKey && spec.autoIncrement ? '' : this.mapDataType(spec.type);
    var len = spec.length ? util.format('(%s)', spec.length) : '';
    var constraint = this.createColumnConstraint(
      spec,
      options,
      tableName,
      name
    );
    if (name.charAt(0) !== '[') {
      name = `[${name}]`;
    }

    return {
      foreignKey: constraint.foreignKey,
      callbacks: constraint.callbacks,
      constraints: [name, type, len, constraint.constraints].join(' ')
    };
  },

  escapeDDL: function (strOrArray) {
    // if its an array we're going to assume it's: [schemaName, tableName]
    if (Array.isArray(strOrArray)) {
      if (strOrArray.length === 2) {
        return `[${strOrArray[0]}].[${strOrArray[1]}]`;
      }
      return `[${strOrArray[0]}]`;
    }
    return `[${strOrArray}]`;
  },

  _translateSpecialDefaultValues: function (
    spec,
    options,
    tableName,
    columnName
  ) {
    switch (spec.defaultValue.special) {
      case 'CURRENT_TIMESTAMP':
        spec.defaultValue.prep = 'CURRENT_TIMESTAMP';
        break;
      default:
        this.super(spec, options, tableName, columnName);
        break;
    }
  },

  mapDataType: function (str) {
    switch (str) {
      case 'json':
      case 'jsonb':
        return str.toUpperCase();
      case this.type.STRING:
        return 'NVARCHAR';
      case this.type.DATE_TIME:
        return 'DATETIME2';
      case this.type.BLOB:
        return 'VARBINARY(MAX)';
    }
    return this._super(str);
  },

  createDatabase: function (dbName, options, callback) {
    var spec = '';

    if (typeof options === 'function') callback = options;

    this.runSql(
      util.format('CREATE DATABASE %s %s', this.escapeDDL(dbName), spec),
      callback
    );
  },

  dropDatabase: function (dbName, options, callback) {
    var ifExists = '';

    if (typeof options === 'function') callback = options;
    else {
      ifExists = options.ifExists === true ? 'IF EXISTS' : '';
    }

    this.runSql(
      util.format('DROP DATABASE %s %s', ifExists, this.escapeDDL(dbName)),
      callback
    );
  },

  createSequence: function (sqName, options, callback) {
    var spec = '';
    var temp = '';

    if (typeof options === 'function') callback = options;
    else {
      temp = options.temp === true ? 'TEMP' : '';
    }

    this.runSql(
      util.format('CREATE %s SEQUENCE [%s] %s', temp, sqName, spec),
      callback
    );
  },

  switchDatabase: function (options, callback) {
    if (typeof options === 'object') {
      if (typeof options.database === 'string') {
        this.log.info(
          'Ignore database option, not available with postgres. Use schema instead!'
        );
        this.runSql(
          util.format('USE [%s]', options.database),
          callback
        );
      }
    } else if (typeof options === 'string') {
      this.runSql(util.format('USE [%s]', options), callback);
    } else callback(null);
  },

  dropSequence: function (dbName, options, callback) {
    var ifExists = '';
    var rule = '';

    if (typeof options === 'function') callback = options;
    else {
      ifExists = options.ifExists === true ? 'IF EXISTS' : '';

      if (options.cascade === true) rule = 'CASCADE';
      else if (options.restrict === true) rule = 'RESTRICT';
    }

    this.runSql(
      util.format('DROP SEQUENCE %s [%s] %s', ifExists, dbName, rule),
      callback
    );
  },

  createMigrationsTable: function (callback) {
    var options = {
      columns: {
        id: {
          type: this.type.INTEGER,
          notNull: true,
          primaryKey: true,
          autoIncrement: true
        },
        name: { type: this.type.STRING, length: 255, notNull: true },
        run_on: { type: this.type.DATE_TIME, notNull: true }
      },
      ifNotExists: false
    };

    return this.all(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_name = '${this.internals.migrationTable}'
        AND table_schema = '${this.schema}'`
    )
      .then(
        function (result) {
          if (result && result.length < 1) {
            return this.createTable([this.schema, this.internals.migrationTable], options);
          } else {
            return Promise.resolve();
          }
        }.bind(this)
      )
      .nodeify(callback);
  },

  createSeedsTable: function (callback) {
    var options = {
      columns: {
        id: {
          type: this.type.INTEGER,
          notNull: true,
          primaryKey: true,
          autoIncrement: true
        },
        name: { type: this.type.STRING, length: 255, notNull: true },
        run_on: { type: this.type.DATE_TIME, notNull: true }
      },
      ifNotExists: false
    };

    return this.all(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_name = '${this.internals.seedTable}'
        AND table_schema = '${this.schema}'`
    )
      .then(
        function (result) {
          if (result && result.length < 1) {
            return this.createTable([this.schema, this.internals.seedTable], options);
          } else {
            return Promise.resolve();
          }
        }.bind(this)
      )
      .nodeify(callback);
  },

  createColumnConstraint: function (spec, options, tableName, columnName) {
    var constraint = [];
    var callbacks = [];
    var cb;

    if (spec.primaryKey) {
      if (spec.autoIncrement) {
        constraint.push('INT IDENTITY');
      }

      if (options.emitPrimaryKey) {
        constraint.push('PRIMARY KEY');
      }
    }

    if (spec.notNull === true) {
      constraint.push('NOT NULL');
    }

    if (spec.unique) {
      constraint.push('UNIQUE');
    }

    if (spec.defaultValue !== undefined) {
      constraint.push('DEFAULT');
      if (typeof spec.defaultValue === 'string') {
        constraint.push("'" + spec.defaultValue + "'");
      } else if (typeof spec.defaultValue.prep === 'string') {
        constraint.push(spec.defaultValue.prep);
      } else {
        constraint.push(spec.defaultValue);
      }
    }

    // keep foreignKey for backward compatiable, push to callbacks in the future
    if (spec.foreignKey) {
      cb = this.bindForeignKey([this.schema, tableName], columnName, spec.foreignKey);
    }

    return {
      foreignKey: cb,
      callbacks: callbacks,
      constraints: constraint.join(' ')
    };
  },

  renameTable: function (tableName, newTableName, callback) {
    var sql = util.format(
      'ALTER TABLE [%s].[%s] RENAME TO [%s]',
      this.schema,
      tableName,
      newTableName
    );
    return this.runSql(sql).nodeify(callback);
  },

  removeColumn: function (tableName, columnName, callback) {
    var sql = util.format(
      'ALTER TABLE [%s].[%s] DROP COLUMN [%s]',
      this.schema,
      tableName,
      columnName
    );

    return this.runSql(sql).nodeify(callback);
  },

  addColumn: function(tableName, columnName, columnSpec, callback) {
    var columnSpec = this.normalizeColumnSpec(columnSpec);
    this._prepareSpec(columnName, columnSpec, {}, tableName);
    var def = this.createColumnDef(columnName, columnSpec, {}, tableName);
    var extensions = '';
    var self = this;

    if (typeof this._applyAddColumnExtension === 'function') {
      extensions = this._applyAddColumnExtension(def, tableName, columnName);
    }

    var sql = util.format(
      'ALTER TABLE %s ADD %s %s',
      this.escapeDDL(tableName),
      def.constraints,
      extensions
    );

    return this.runSql(sql)
      .then(function() {
        var callbacks = def.callbacks || [];
        if (def.foreignKey) callbacks.push(def.foreignKey);
        return self.recurseCallbackArray(callbacks);
      })
      .nodeify(callback);
  },

  renameColumn: function (tableName, oldColumnName, newColumnName, callback) {
    var sql = `
      IF EXISTS (
        SELECT 1
        FROM sys.columns
        WHERE name = '${oldColumnName}'
          AND object_name(object_id) = '${this.escapeDDL(tableName)}'
      ) AND NOT EXISTS (
        SELECT 1
        FROM sys.columns
        WHERE name = '${newColumnName}'
          AND object_name(object_id) = '${this.escapeDDL(tableName)}'
      )
        EXEC sp_RENAME
          '${this.escapeDDL(this.schema)}.${this.escapeDDL(tableName)}.${oldColumnName}',
          '${newColumnName}',
          'COLUMN';
    `;
    return this.runSql(sql).nodeify(callback);
  },

  changeColumn: function (tableName, columnName, columnSpec, callback) {
    return setNotNull.call(this);

    function setNotNull () {
      var setOrDrop = columnSpec.notNull === true ? 'SET' : 'DROP';
      var sql = util.format(
        'ALTER TABLE [%s].[%s] ALTER COLUMN [%s] %s NOT NULL',
        this.schema,
        tableName,
        columnName,
        setOrDrop
      );

      return this.runSql(sql).nodeify(setUnique.bind(this));
    }

    function setUnique (err) {
      if (err) {
        return Promise.reject(err);
      }

      var sql;
      var constraintName = tableName + '_' + columnName + '_key';

      if (columnSpec.unique === true) {
        sql = util.format(
          'ALTER TABLE [%s].[%s] ADD CONSTRAINT [%s] UNIQUE ([%s])',
          this.schema,
          tableName,
          constraintName,
          columnName
        );
        return this.runSql(sql).nodeify(setDefaultValue.bind(this));
      } else if (columnSpec.unique === false) {
        sql = util.format(
          'ALTER TABLE [%s].[%s] DROP CONSTRAINT [%s]',
          this.schema,
          tableName,
          constraintName
        );
        return this.runSql(sql).nodeify(setDefaultValue.bind(this));
      } else {
        return setDefaultValue.call(this);
      }
    }

    function setDefaultValue (err) {
      if (err) {
        return Promise.reject(err).nodeify(callback);
      }

      var sql;

      if (columnSpec.defaultValue !== undefined) {
        var defaultValue = null;
        if (typeof columnSpec.defaultValue === 'string') {
          defaultValue = "'" + columnSpec.defaultValue + "'";
        } else {
          defaultValue = columnSpec.defaultValue;
        }
        sql = util.format(
          'ALTER TABLE [%s].[%s] ALTER COLUMN [%s] SET DEFAULT %s',
          this.schema,
          tableName,
          columnName,
          defaultValue
        );
      } else {
        sql = util.format(
          'ALTER TABLE [%s].[%s] ALTER COLUMN [%s] DROP DEFAULT',
          this.schema,
          tableName,
          columnName
        );
      }
      return this.runSql(sql)
        .then(setType.bind(this))
        .nodeify(callback);
    }

    function setType () {
      if (columnSpec.type !== undefined) {
        var using =
          columnSpec.using !== undefined
            ? columnSpec.using
            : util.format(
              'USING [%s]::%s',
              columnName,
              this.mapDataType(columnSpec.type)
            );
        var len = columnSpec.length
          ? util.format('(%s)', columnSpec.length)
          : '';
        var sql = util.format(
          'ALTER TABLE [%s].[%s] ALTER COLUMN [%s] TYPE %s %s %s',
          this.schema,
          tableName,
          columnName,
          this.mapDataType(columnSpec.type),
          len,
          using
        );
        return this.runSql(sql);
      }
    }
  },

  addForeignKey: function (
    tableName,
    referencedTableName,
    keyName,
    fieldMapping,
    rules,
    callback
  ) {
    if (arguments.length === 5 && typeof rules === 'function') {
      callback = rules;
      rules = {};
    }
    var columns = Object.keys(fieldMapping);
    var referencedColumns = columns.map(function (key) {
      return '"' + fieldMapping[key] + '"';
    });
    var sql = util.format(
      'ALTER TABLE [%s].[%s] ADD CONSTRAINT [%s] FOREIGN KEY (%s) REFERENCES [%s] (%s) ON DELETE %s ON UPDATE %s',
      this.schema,
      tableName,
      keyName,
      this.quoteDDLArr(columns),
      referencedTableName,
      referencedColumns,
      rules.onDelete || 'NO ACTION',
      rules.onUpdate || 'NO ACTION'
    );
    return this.runSql(sql).nodeify(callback);
  },

  removeForeignKey: function (tableName, keyName, callback) {
    var sql = util.format(
      'ALTER TABLE [%s].[%s] DROP CONSTRAINT [%s]',
      this.schema,
      tableName,
      keyName
    );
    return this.runSql(sql).nodeify(callback);
  },

  insert: function () {
    var index = 1;

    if (arguments.length > 3) {
      index = 2;
    }

    arguments[index] = arguments[index].map(function (value) {
      return typeof value === 'string' ? value : JSON.stringify(value);
    });

    return this._super.apply(this, arguments);
  },

  runSql: function () {
    var callback;
    var minLength = 1;
    var params;

    if (typeof arguments[arguments.length - 1] === 'function') {
      minLength = 2;
      callback = arguments[arguments.length - 1];
    }

    params = arguments;

    if (params.length > minLength) {
      // We have parameters, but db-migrate uses "?" for param substitutions.
      // MSSQL uses "@paramname1", "@paramname2", etc so fix up the "?" into "$1", etc
      var param = params[0].split('?');
      var newParam = [];
      for (var i = 0; i < param.length - 1; i++) {
        newParam.push(param[i], '@input_' + (i + 1));
      }
      newParam.push(param[param.length - 1]);
      params[0] = newParam.join('');
    }

    this.log.sql.apply(null, params);
    if (this.internals.dryRun) {
      return Promise.resolve().nodeify(callback);
    }

    return new Promise(
      function (resolve, reject) {
        if (this.internals.notransactions) {
          const request = new this.connection.Request();

          if (params[1]) {
            for (let i = 0; i < params[1].length; i++) {
              request.input(`input_${i + 1}`, params[1][i]);
            }
          }

          request.query(params[0]).then(result => {
            resolve(result.recordset);
          }, reject);
        }
        else {
          const transaction = new this.connection.Transaction();
          transaction.begin(() => {
            const request = new this.connection.Request(transaction);

            if (params[1]) {
              for (let i = 0; i < params[1].length; i++) {
                request.input(`input_${i + 1}`, params[1][i]);
              }
            }

            request.query(params[0]).then(result => {
              transaction.commit();
              resolve(result.recordset);
            }, reject);
          })
        }
      }.bind(this)
    ).nodeify(callback);
  },

  all: function () {
    var params = arguments;

    this.log.sql.apply(null, params);

    return new Promise(
      function (resolve, reject) {
        const request = new this.connection.Request();

        request.query(params[0]).then(result => {
          resolve(result.recordset);
        }, reject);
      }.bind(this)
    ).nodeify(params[1]);
  },

  close: function (callback) {
    const promise = new Promise((resolve) => {
      // circumvent Idle connections not being closed by the pool #457
      // https://github.com/tediousjs/node-mssql/issues/457
      setTimeout(resolve, 200);
    }).then(() => {
      return this.connection.close();
    });

    if (typeof callback === 'function') {
      return Promise.resolve(promise).nodeify(callback);
    } else return Promise.resolve(promise);
  },

  /**
   * Extend Base because they do not let schema to be passed
  */
  addMigrationRecord: function (name, callback) {
    this.runSql(
      'INSERT INTO ' +
        this.escapeDDL([this.schema, this.internals.migrationTable]) +
        ' (' +
        this.escapeDDL('name') +
        ', ' +
        this.escapeDDL('run_on') +
        ') VALUES (?, ?)',
      [name, new Date()],
      callback
    );
  },
  addSeedRecord: function (name, callback) {
    this.runSql(
      'INSERT INTO ' +
        this.escapeDDL([this.schema, this.internals.seedTable]) +
        ' (' +
        this.escapeDDL('name') +
        ', ' +
        this.escapeDDL('run_on') +
        ') VALUES (?, ?)',
      [name, new Date()],
      callback
    );
  },
  allLoadedMigrations: function (callback) {
    var sql =
      'SELECT * FROM ' +
      this.escapeDDL([this.schema, this.internals.migrationTable]) +
      ' ORDER BY run_on DESC, name DESC';
    return this.all(sql, callback);
  },
  allLoadedSeeds: function (callback) {
    var sql =
      'SELECT * FROM ' +
      this.escapeDDL([this.schema, this.internals.seedTable]) +
      ' ORDER BY run_on DESC, name DESC';
    return this.all(sql, callback);
  },
  deleteMigration: function (migrationName, callback) {
    var sql =
      'DELETE FROM ' +
      this.escapeDDL([this.schema, this.internals.migrationTable]) +
      ' WHERE name = ?';
    this.runSql(sql, [migrationName], callback);
  }
});

Promise.promisifyAll(MssqlDriver);

exports.connect = function (config, intern, callback) {
  if (!config.database) {
    config.database = 'mssql';
  }
  config.server = config.server || config.host;


  mssql.connect(config)
    .then(pool => {
      callback(null, new MssqlDriver(mssql, config.schema, intern));
    })
    .catch(err => {
      callback(err);
    });
};

exports.base = MssqlDriver;
