using Disposable;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace DataReaderFactory.Tests
{
    public class Tests
    {
        private static readonly DisposableLocalDb LocalDb = new("TestDb");
        internal static string TestDb => LocalDb.ConnectionString;
        private static readonly DataReaderBuilder<int> Builder = new DataReaderBuilder<int>
        {
            x => x,
            x => 0,
            x => 0,
            x => 0,
        };
            

        [OneTimeSetUp]
        public static void Setup()
        {
            using var conn = new SqlConnection(TestDb);
            conn.Open();
            using var cmd = new SqlCommand(@"create table dbo.ids(id int not null primary key clustered,timestamp);
create type dbo.tvp as table(id int not null /*primary key clustered*/
,unused float
,unused1 float
,unused2 float);
with t as (select*from string_split(space(1414),' '))
insert into ids(id)
select row_number() over (order by 1/0)
from t, t b", conn);
            cmd.ExecuteNonQuery();
            Builder.CreateReader(Enumerable.Empty<int>());
        }

        [Test]
        public static async Task TestTVP()
        {
            using var conn = new SqlConnection(TestDb);
            conn.Open();
            using var cmd = new SqlCommand(@";select i.id from ids i where exists (select 1 from @tvp t where i.id = t.id)", conn);
            var parameter = cmd.Parameters.Add(new SqlParameter("@tvp", Builder.CreateReader(Enumerable.Range(1_000_000, 100_000))){
                SqlDbType = SqlDbType.Structured,
                TypeName = "dbo.tvp"
            });
            using var rdr = await cmd.ExecuteReaderAsync();
            Assert.That(rdr.Cast<object>().Count(), Is.EqualTo(100_000));
        }

        [Test]
        public static async Task TestTVPUsingTemp()
        {
            using var conn = new SqlConnection(TestDb);
            conn.Open();
            using var cmd = new SqlCommand(@"
create table #t(id int not null /*primary key clustered*/,unused float,unused1 float,unused2 float);
insert into #t select * from @tvp;select i.id from ids i where exists (select 1 from #t t where i.id = t.id)", conn);
            var parameter = cmd.Parameters.Add(new SqlParameter("@tvp", Builder.CreateReader(Enumerable.Range(1_000_000, 100_000)))
            {
                SqlDbType = SqlDbType.Structured,
                TypeName = "dbo.tvp"
            });
            using var rdr = await cmd.ExecuteReaderAsync();
            Assert.That(rdr.Cast<object>().Count(), Is.EqualTo(100_000));
        }

        [Test]
        public static async Task TestBulk()
        {
            using var conn = new SqlConnection(TestDb);
            conn.Open();
            using var cmd = new SqlCommand("create table #t(id int not null /*primary key clustered*/,unused float,unused1 float,unused2 float)", conn);
            await cmd.ExecuteNonQueryAsync();
            using var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null)
            {
                EnableStreaming = true,
                BatchSize = 10_000,
                DestinationTableName = "#t",
                ColumnMappings = {new(0, 0)},
                ColumnOrderHints = {new("id", SortOrder.Ascending)}
                
            };
            await bulk.WriteToServerAsync(Builder.CreateReader(Enumerable.Range(1_000_000, 100_000)));
            using var cmd2 = new SqlCommand("select i.id from ids i where exists (select 1 from #t t where i.id = t.id)", conn);
            using var rdr = await cmd2.ExecuteReaderAsync();
            Assert.That(rdr.Cast<object>().Count(), Is.EqualTo(100_000));
        }

        [OneTimeTearDown]
        public static void TearDown()
        {
            //LocalDb.Dispose();
            Builder.Dispose();
        }
    }
}