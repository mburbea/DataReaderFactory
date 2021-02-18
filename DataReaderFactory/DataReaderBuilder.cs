namespace DataReaderFactory
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Linq;
    using System.Linq.Expressions;

    public sealed partial class DataReaderBuilder<T> : IEnumerable, IDisposable
    {
        private static ParameterExpression ObjArrayParam => Helpers.ObjArrayParam;
        private static ParameterExpression StateParam => Helpers.StateParam;
        private static ParameterExpression ItemParam { get; }= Expression.Parameter(typeof(T), "item");
        private static readonly ParameterReplacerVistor ParameterReplacer = new(ItemParam, StateParam);

        private readonly List<Expression> _body = new();
        private Action<object[], T, object> _populate;
        private DataTable _schemaTable;

        private DataTable SchemaTable => Helpers.EnsurePopulated(ref _schemaTable, static state => CreateSchemaTable(state), _body);

        private Action<object[], T, object> Populate => Helpers.EnsurePopulated(ref _populate, static state => CreatePopulateAction(state), _body);

        private static Action<object[], T, object> CreatePopulateAction(List<Expression> body) => Expression.Lambda<Action<object[], T, object>>(
            Expression.Block(
                body.Select(static (expr, i) => Expression.Assign(
                    Expression.ArrayAccess(ObjArrayParam, Expression.Constant(i)),
                    expr.Type.IsValueType 
                        ? Expression.Convert(expr, typeof(object)) 
                        : expr))),
            new[] { ObjArrayParam, ItemParam, StateParam }
        ).Compile();

        private static DataTable CreateSchemaTable(List<Expression> body) 
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
                var utype = Nullable.GetUnderlyingType(type) ?? type;
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
