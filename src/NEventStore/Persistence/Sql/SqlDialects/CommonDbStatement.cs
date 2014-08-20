namespace NEventStore.Persistence.Sql.SqlDialects
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Reactive;
    using System.Reactive.Linq;
    using System.Threading.Tasks;
    using System.Transactions;
    using NEventStore.Logging;

    public class CommonDbStatement : IDbStatement
    {
        private const int InfinitePageSize = 0;
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof (CommonDbStatement));
        private readonly DbConnection _connection;
        private readonly ISqlDialect _dialect;
        private readonly TransactionScope _scope;
        private readonly IDbTransaction _transaction;
        private static readonly NextPageDelegate None = (command, current) => { };

        public CommonDbStatement(
            ISqlDialect dialect,
            TransactionScope scope,
            DbConnection connection,
            IDbTransaction transaction)
        {
            Parameters = new Dictionary<string, Tuple<object, DbType?>>();

            _dialect = dialect;
            _scope = scope;
            _connection = connection;
            _transaction = transaction;
        }

        protected IDictionary<string, Tuple<object, DbType?>> Parameters { get; private set; }

        protected ISqlDialect Dialect
        {
            get { return _dialect; }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public virtual int PageSize { get; set; }

        public virtual void AddParameter(string name, object value, DbType? parameterType = null)
        {
            Logger.Debug(Messages.AddingParameter, name);
            Parameters[name] = Tuple.Create(_dialect.CoalesceParameterValue(value), parameterType);
        }

        public Task<int> ExecuteNonQueryAsync(string commandText)
        {
            try
            {
                using (DbCommand command = BuildCommand(commandText))
                    return command.ExecuteNonQueryAsync();
            }
            catch (Exception e)
            {
                if (_dialect.IsDuplicate(e))
                {
                    throw new UniqueKeyViolationException(e.Message, e);
                }

                throw;
            }
        }

        public virtual int ExecuteWithoutExceptions(string commandText)
        {
            try
            {
                return ExecuteNonQuery(commandText);
            }
            catch (Exception)
            {
                Logger.Debug(Messages.ExceptionSuppressed);
                return 0;
            }
        }

        public Task<int> ExecuteWithoutExceptionsAsync(string commandText)
        {
            try
            {
                return ExecuteNonQueryAsync(commandText);
            }
            catch (Exception)
            {
                Logger.Debug(Messages.ExceptionSuppressed);
                return Task.FromResult(0);
            }
        }

        public virtual int ExecuteNonQuery(string commandText)
        {
            try
            {
                using (IDbCommand command = BuildCommand(commandText))
                    return command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                if (_dialect.IsDuplicate(e))
                {
                    throw new UniqueKeyViolationException(e.Message, e);
                }

                throw;
            }
        }

        public virtual object ExecuteScalar(string commandText)
        {
            try
            {
                using (DbCommand command = BuildCommand(commandText))
                    return command.ExecuteScalar();
            }
            catch (Exception e)
            {
                if (_dialect.IsDuplicate(e))
                {
                    throw new UniqueKeyViolationException(e.Message, e);
                }
                throw;
            }
        }

        public async Task<object> ExecuteScalarAsync(string commandText)
        {
            try
            {
                using (DbCommand command = BuildCommand(commandText))
                {
                    object o = await command.ExecuteScalarAsync();
                    return o;
                }
            }
            catch (Exception e)
            {
                if (_dialect.IsDuplicate(e))
                {
                    throw new UniqueKeyViolationException(e.Message, e);
                }
                throw;
            }
        }

        public virtual IObservable<IDataRecord> ExecuteWithQuery(string queryText)
        {
            return ExecuteQuery(queryText, None, InfinitePageSize);
        }

        public virtual IObservable<IDataRecord> ExecutePagedQuery(string queryText, NextPageDelegate nextpage = null)
        {
            nextpage = nextpage ?? _dialect.NextPageDelegate;

            int pageSize = _dialect.CanPage ? PageSize : InfinitePageSize;
            if (pageSize > 0)
            {
                Logger.Verbose(Messages.MaxPageSize, pageSize);
                Parameters.Add(_dialect.Limit, Tuple.Create((object)pageSize, (DbType?)null));
            }

            return ExecuteQuery(queryText, nextpage, pageSize);
        }

        protected virtual IObservable<IDataRecord> ExecuteQuery(string queryText, NextPageDelegate nextPage, int pageSize)
        {
            return Observable.Create<IDataRecord>(async (observer) =>
            {
                int skip = 0;

                Parameters.Add(_dialect.Skip, Tuple.Create((object)skip, (DbType?)null));

                DbCommand command = BuildCommand(queryText);
                
                for (;;)
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        bool readRecords = false;
                        while (await reader.ReadAsync())
                        {
                            IDataRecord record = reader;

                            observer.OnNext(record);

                            readRecords = true;

                            nextPage(command, record);
                        }

                        if (pageSize == InfinitePageSize || false == readRecords)
                        {
                            break;
                        }

                        skip += pageSize;

                        command.SetParameter(_dialect.Skip, skip);
                    }

                }
                observer.OnCompleted();
            });
        }

        protected virtual void Dispose(bool disposing)
        {
            Logger.Verbose(Messages.DisposingStatement);

            if (_transaction != null)
            {
                _transaction.Dispose();
            }

            if (_connection != null)
            {
                _connection.Dispose();
            }

            if (_scope != null)
            {
                _scope.Dispose();
            }
        }

        protected virtual DbCommand BuildCommand(string statement)
        {
            Logger.Verbose(Messages.CreatingCommand);
            DbCommand command = _connection.CreateCommand();
            ((IDbCommand) command).Transaction = _transaction;
            command.CommandText = statement;

            Logger.Verbose(Messages.ClientControlledTransaction, _transaction != null);
            Logger.Verbose(Messages.CommandTextToExecute, statement);

            BuildParameters(command);

            return command;
        }

        protected virtual void BuildParameters(IDbCommand command)
        {
            foreach (var item in Parameters)
            {
                BuildParameter(command, item.Key, item.Value.Item1, item.Value.Item2);
            }
        }

        protected virtual void BuildParameter(IDbCommand command, string name, object value, DbType? dbType)
        {
            IDbDataParameter parameter = command.CreateParameter();
            parameter.ParameterName = name;
            SetParameterValue(parameter, value, dbType);

            Logger.Verbose(Messages.BindingParameter, name, parameter.Value);
            command.Parameters.Add(parameter);
        }

        protected virtual void SetParameterValue(IDataParameter param, object value, DbType? type)
        {
            param.Value = value ?? DBNull.Value;
            param.DbType = type ?? (value == null ? DbType.Binary : param.DbType);
        }
    }
}