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
            x => x
        };
            

        [OneTimeSetUp]
        public static void Setup()
        {
            using var conn = new SqlConnection(TestDb);
            conn.Open();
            using var cmd = new SqlCommand(@"create table dbo.ids(id int not null);
create type dbo.tvp as table(id int not null);
with t as (select*from string_split(space(255),' '))
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
            using var cmd = new SqlCommand("select * from ids where id in (select id from @tvp)", conn);
            var parameter = cmd.Parameters.AddWithValue("@tvp", Builder.CreateReader(Enumerable.Range(1, 4000)));
            parameter.SqlDbType = SqlDbType.Structured;
            parameter.TypeName = "dbo.tvp";
            using var rdr = await cmd.ExecuteReaderAsync();
            Assert.That(rdr.Cast<object>().Count(), Is.EqualTo(4000));
        }

        [Test]
        public static async Task TestTZ()
        {
            using var conn = new SqlConnection(TestDb);
            conn.Open();
            using var cmd = new SqlCommand("create table #t(id int not null)", conn);
            await cmd.ExecuteNonQueryAsync();
            using var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null)
            {
                EnableStreaming = true,
                BatchSize = 1_000,
                DestinationTableName = "tempdb..#t",
            };
            await bulk.WriteToServerAsync(Builder.CreateReader(Enumerable.Range(1, 4000)));
            using var cmd2 = new SqlCommand("select * from ids where id in (select id from #t)", conn);
            using var rdr = await cmd2.ExecuteReaderAsync();
            Assert.That(rdr.Cast<object>().Count(), Is.EqualTo(4000));
        }

        [OneTimeTearDown]
        public static void TearDown()
        {
            LocalDb.Dispose();
        }
    }
}