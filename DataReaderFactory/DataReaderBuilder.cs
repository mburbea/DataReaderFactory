using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace DataReaderFactory
{
    public sealed partial class DataReaderBuilder<T> : IEnumerable, IDisposable
    {
        private static readonly ParameterExpression ObjArrayParam = Expression.Variable(typeof(object[]), "data");
        private static readonly ParameterExpression StateParam = Expression.Parameter(typeof(object), "state");
        private static readonly ParameterExpression ItemParam = Expression.Parameter(typeof(T), "item");
        private static readonly ParameterReplacerVistor ParameterReplacer = new(ItemParam, StateParam);

        private readonly List<Expression> _body = new();
        private Action<object[], T, object> _populate;
        private DataTable _schemaTable;

        private DataTable SchemaTable => Volatile.Read(ref _schemaTable) ?? EnsurePopulated(ref _schemaTable);

        private Action<object[], T, object> Populate => Volatile.Read(ref _populate) ?? EnsurePopulated(ref _populate);

        private Action<object[], T, object> EnsurePopulated(ref Action<object[], T, object> populate)
        {
            Interlocked.CompareExchange(ref populate, Expression.Lambda<Action<object[], T, object>>(
                Expression.Block(_body.Select(static (expr, i) => Expression.Assign(
                    Expression.ArrayAccess(ObjArrayParam, Expression.Constant(i))
                    , expr.Type.IsValueType ? Expression.Convert(expr, typeof(object)) : expr
                ))),
                ObjArrayParam,
                ItemParam,
                StateParam
            ).Compile(), null);
            return populate;
        }

        private DataTable EnsurePopulated(ref DataTable schemaTable)
        {
            Interlocked.CompareExchange(ref schemaTable, CreateSchemaTable(_body), null);
            return schemaTable;

            static DataTable CreateSchemaTable(List<Expression> body)
            {
                var schemaTable = new DataTable(nameof(SchemaTable))
                {
                    Columns =
                    {
                        { SchemaTableColumn.ColumnOrdinal, typeof(int) },
                        { SchemaTableColumn.ColumnName, typeof(string) },
                        { SchemaTableColumn.DataType, typeof(Type) },
                        { SchemaTableColumn.ColumnSize, typeof(int) },
                        { SchemaTableColumn.AllowDBNull, typeof(bool) },
                        { SchemaTableColumn.IsKey, typeof(bool) }
                    }
                };

                for (int i = 0; i < body.Count; i++)
                {
                    var type = body[i].Type;
                    var utype = Nullable.GetUnderlyingType(body[i].Type) ?? body[i].Type;
                    var isNullable = !type.IsValueType || utype != type;
                    schemaTable.Rows.Add(
                        i,
                        $"c{i}",
                        utype,
                        -1,
                        isNullable,
                        false);
                }
                return schemaTable;
            }
        }

        public void Add<TProperty>(Expression<Func<T, TProperty>> expression) => Add((LambdaExpression)expression);

        public void Add<TProperty>(Expression<Func<T, object, TProperty>> expression) => Add((LambdaExpression)expression);

        public void Add(LambdaExpression expression) => Add(expression?.Body);

        public void Add(string propertyOrFieldName) => Add(Expression.PropertyOrField(ItemParam, propertyOrFieldName ?? throw new ArgumentNullException(nameof(propertyOrFieldName))));

        public void Add(Expression expression)
        {
            if (expression is null)
            {
                throw new ArgumentNullException(nameof(expression));
            }
            if (_populate is not null)
            {
                throw new InvalidOperationException("A DataReader has already been created");
            }
            _body.Add(ParameterReplacer.Visit(expression));
        }

        public DbDataReader CreateReader(IEnumerable<T> source, object state = null) => new DataReader(this, new AsyncWrapper<T>(source.GetEnumerator()), state, _body.Count, Populate);
        
        public DbDataReader CreateReader(IAsyncEnumerable<T> source, object state = null) => new DataReader(this, source.GetAsyncEnumerator(), state, _body.Count, Populate);

        IEnumerator IEnumerable.GetEnumerator() => throw new NotImplementedException();

        public void Dispose() => _schemaTable?.Dispose();
    }
}
