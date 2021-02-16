using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace DataReaderFactory
{
    public sealed partial class DataReaderBuilder<T>
    {
        private sealed class DataReader : DbDataReader
        {
            private readonly IAsyncEnumerator<T> _asyncEnumerator;
            private readonly object[] _array;
            private readonly object _state;
            private readonly Action<object[], T, object> _populate;
            private DataReaderBuilder<T> _parent;

            /// <summary>
            /// Creates a new instance of the <see cref="DataReader" /> classes.
            /// </summary>
            /// <param name="schemaTable">The schema table.</param>
            /// <param name="populateValues">The delegate to use to populate an array of values from the current item on the enumerator.</param>
            /// <param name="enumerator">The enumerator of items.</param>
            public DataReader(DataReaderBuilder<T> parent, IAsyncEnumerator<T> asyncEnumerator, object state, int fieldCount, Action<object[], T, object> populate) =>
                (_parent, _asyncEnumerator, _state, _array, _populate) = (parent, asyncEnumerator, state, new object[fieldCount], populate);

            public override int FieldCount => _array.Length;

            public override int Depth => 0;

            public override bool HasRows => _parent is not null;

            public override bool IsClosed => _parent is null;

            public override int RecordsAffected => 0;

            public override object this[int ordinal] => _array[ordinal];

            public override object this[string name] => uint.TryParse(name.AsSpan(1), out var ordinal) && ordinal < (uint)_array.Length ? _array[ordinal] : null;

            public override void Close()
            {
                _asyncEnumerator.DisposeAsync();
                _parent = null;
            }

            public override bool Read() => ReadCore().GetAwaiter().GetResult();

            public override Task<bool> ReadAsync(CancellationToken cancellationToken) => ReadCore().AsTask();

            private async ValueTask<bool> ReadCore()
            {
                if (await _asyncEnumerator.MoveNextAsync().ConfigureAwait(false))
                {
                    _populate(_array, _asyncEnumerator.Current, _state);
                    return true;
                }
                Close();
                return false;
            }

            public override bool GetBoolean(int ordinal) => (bool)_array[ordinal];

            public override byte GetByte(int ordinal) => (byte)_array[ordinal];

            public override char GetChar(int ordinal) => (char)_array[ordinal];

            public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) =>
                GetSubsetOfT(_array[ordinal] switch{
                byte[] bytes => new(bytes),
                _ => new()
                },(int)dataOffset, buffer, bufferOffset, length);

            public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) =>
                GetSubsetOfT(_array[ordinal] switch
                {
                    string s => s,
                    char[] chars => chars,
                    _ => new()
                }, (int)dataOffset, buffer, bufferOffset, length);

            private long GetSubsetOfT<TArray>(ReadOnlySpan<TArray> sourceData, int offset, TArray[] buffer, int bufferOffset, int length)
            {
                var available = sourceData.Length - offset;
                if (available > 0)
                {
                    int count = Math.Min(length, available);
                    sourceData.Slice(offset, count).CopyTo(buffer.AsSpan(bufferOffset, count));
                    return count;
                }
                return 0;
            }

            public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;

            public override DateTime GetDateTime(int ordinal) => (DateTime)_array[ordinal];

            public override decimal GetDecimal(int ordinal) => (decimal)_array[ordinal];

            public override double GetDouble(int ordinal) => (double)_array[ordinal];

            public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: true);

            public override Type GetFieldType(int ordinal) => _array[ordinal]?.GetType() ?? (Type)GetSchemaTable().Rows[ordinal][SchemaTableColumn.DataType];

            public override float GetFloat(int ordinal) => (float)_array[ordinal];

            public override Guid GetGuid(int ordinal) => (Guid)_array[ordinal];

            public override short GetInt16(int ordinal) => (short)_array[ordinal];

            public override int GetInt32(int ordinal) => (int)_array[ordinal];

            public override long GetInt64(int ordinal) => (long)_array[ordinal];

            public override string GetName(int ordinal) => ordinal < _array.Length ? $"c{ordinal}" : null;

            public override int GetOrdinal(string name) => uint.TryParse(name.AsSpan(1), out var ordinal) && ordinal < (uint)_array.Length ? (int)ordinal : -1;

            public override string GetString(int ordinal) => _array[ordinal]?.ToString();

            public override object GetValue(int ordinal) => _array[ordinal];

            public override int GetValues(object[] values)
            {
                var amount = Math.Min(values.Length, _array.Length);
                _array.AsSpan(0, amount).CopyTo(values);
                return amount;
            }

            public override bool IsDBNull(int ordinal) => _array[ordinal] is null;

            public override bool NextResult()
            {
                Close();
                return false;
            }

            public override DataTable GetSchemaTable() => _parent.SchemaTable;
        }
    }
}
