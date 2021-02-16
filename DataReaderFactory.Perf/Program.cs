using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Disposable;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace DataReaderFactory.Perf
{
    class Program
    {
        static void Main(string[] args)
        {
            BenchmarkRunner.Run<TvpVsBulk>();
        }
    }

    public class TvpVsBulk
    {
        private static readonly DisposableLocalDb LocalDb = new("TestDb");
        string ConnectionString => LocalDb.ConnectionString;
        protected static DataReaderBuilder<int> Builder { get; } = new DataReaderBuilder<int> { x => x };
        protected static SqlMetaData[] _metaData = { new("id", SqlDbType.Int, false, true, SortOrder.Ascending, 0) };

        [Params(100_000, 1_000_000)]
        public int Initial { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            using var conn = new SqlConnection(ConnectionString);
            conn.Open();
            using var cmd = new SqlCommand(@"create table dbo.ids(id int not null primary key clustered);
                create type dbo.tvp as table(id int not null primary key clustered);
                with t as (select*from string_split(space(1414),' '))
                insert into ids(id)
                select row_number() over (order by 1/0)
                from t, t b", conn);
            cmd.ExecuteNonQuery();
            Builder.CreateReader(Array.Empty<int>()).Read();
        }

        [Benchmark]
        public async Task<int> CsvAsync()
        {
            using var conn = new SqlConnection(ConnectionString);
            conn.Open();
            using var cmd = new SqlCommand("select count(*) from ids where id in (select convert(int,value) from string_split(@tvp,','))", conn);
            var parameter = cmd.Parameters.AddWithValue("@tvp", string.Join(',', Enumerable.Range(Initial, 10_000)));
            parameter.SqlDbType = SqlDbType.VarChar;
            parameter.Size = -1;
            return (int)await cmd.ExecuteScalarAsync().ConfigureAwait(false);
        }

        [Benchmark]
        public async Task<int> TvpAsync()
        {
            using var conn = new SqlConnection(ConnectionString);
            conn.Open();
            using var cmd = new SqlCommand("select count(*) from ids where id in (select id from @tvp)", conn);
            var parameter = cmd.Parameters.AddWithValue("@tvp", Iterator(Initial));
            parameter.SqlDbType = SqlDbType.Structured;
            parameter.TypeName = "dbo.tvp";
            return (int)await cmd.ExecuteScalarAsync().ConfigureAwait(false);

            static IEnumerable<SqlDataRecord> Iterator(int initial)
            {
                var record = new SqlDataRecord(_metaData);
                record.SetInt32(0, initial);
                for (int i = 0; i < 10_000; i++)
                {
                    record.SetInt32(0, initial + i);
                    yield return record;
                }
            }
        }

        [Benchmark]
        public async Task<int> BulkAsync()
        {
            using var conn = new SqlConnection(ConnectionString);
            conn.Open();
            using var prep = new SqlCommand("create table #t(id int not null primary key clustered)", conn);
            await prep.ExecuteNonQueryAsync().ConfigureAwait(false);
            using var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null)
            {
                EnableStreaming = false,
                BatchSize = 10_000,
                DestinationTableName = "tempdb..#t",
                ColumnMappings = { new(0, 0) },
                ColumnOrderHints = { new("id", SortOrder.Ascending) }
            };
            await bulk.WriteToServerAsync(Builder.CreateReader(Enumerable.Range(Initial, 10_000))).ConfigureAwait(false);
            using var cmd = new SqlCommand("select count(*) from ids where id in (select id from #t)", conn);
            return (int)await cmd.ExecuteScalarAsync().ConfigureAwait(false);
        }

        [GlobalCleanup]
        public void CleanUp()
        {
            LocalDb?.Dispose();
            Builder?.Dispose();
        }
    }
}
