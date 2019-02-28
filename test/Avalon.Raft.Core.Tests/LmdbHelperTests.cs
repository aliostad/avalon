using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Spreads.LMDB;
using Xunit;
using Avalon.Common;

namespace Avalon.Raft.Core.Tests
{
    public class LmdbHelperTests
    {
        private readonly string _directory;
        private LMDBEnvironment _env;
        const string DatabaseName = "simit";

        public LmdbHelperTests()
        {
            _directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            _env = LMDBEnvironment.Create();
            _env.Open();
        }

        [Fact]
        public void Can_Save_And_Read_String()
        {
            string key = "jam!";
            string value = "jim!";

            using (var db = _env.OpenDatabase(DatabaseName, new DatabaseConfig(DbFlags.Create)))
            {
                using (var tx = _env.BeginTransaction())
                {
                    tx.Put(db, key, value);
                    tx.Commit();
                }

                using (var tx = _env.BeginReadOnlyTransaction())
                {
                    Bufferable b = default;
                    var success = tx.TryGet(db, key, out b);
                    Assert.True(success);
                    Assert.Equal(value, b);
                }
            }
        }

        [Fact]
        public void Can_Save_And_Read_Long()
        {
            var key = 42L;
            var value = 1969L;

            using (var db = _env.OpenDatabase(DatabaseName, new DatabaseConfig(DbFlags.Create)))
            {
                using (var tx = _env.BeginTransaction())
                {
                    tx.Put(db, key, value);
                    tx.Commit();
                }

                using (var tx = _env.BeginReadOnlyTransaction())
                {
                    Bufferable b = default;
                    var success = tx.TryGet(db, key, out b);
                    Assert.True(success);
                    Assert.Equal(value, (long) b);
                }
            }
        }

        [Fact]
        public void Can_Save_And_Read_int_guid()
        {
            var key = 42;
            var value = Guid.NewGuid();

            using (var db = _env.OpenDatabase(DatabaseName, new DatabaseConfig(DbFlags.Create)))
            {
                using (var tx = _env.BeginTransaction())
                {
                    tx.Put(db, key, value);
                    tx.Commit();
                }

                using (var tx = _env.BeginReadOnlyTransaction())
                {
                    Bufferable b = default;
                    var success = tx.TryGet(db, key, out b);
                    Assert.True(success);
                    Assert.Equal(value, (Guid) b);
                }
            }
        }

        ~LmdbHelperTests()
        {
            _env.Close();
            Directory.Delete(_directory, true);
        }
    }
}
