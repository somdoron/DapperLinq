using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace DapperLinq.Tests
{
    [TestFixture]
    public class QueryTests
    {
        private const string ConnectionString =  @"Data Source=(localdb)\v11.0;Initial Catalog=DapperLinqTests;Integrated Security=True";        

        [SetUp]
        public void Setup()
        {
            using (SqlConnection connection = new SqlConnection(@"Data Source=(localdb)\v11.0;Integrated Security=True"))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText =
                        "IF NOT EXISTS ( SELECT [name] FROM sys.databases WHERE [name] = 'DapperLinqTests' ) CREATE DATABASE DapperLinqTests";
                    command.ExecuteNonQuery();

                    connection.ChangeDatabase("DapperLinqTests");

                    command.CommandType = CommandType.Text;
                    command.CommandText = "IF EXISTS ( SELECT [name] FROM sys.tables WHERE [name] = 'Person' ) DROP Table Person";
                    command.ExecuteNonQuery();

                    command.CommandType = CommandType.Text;
                    command.CommandText = "IF EXISTS ( SELECT [name] FROM sys.tables WHERE [name] = 'Country' ) DROP Table Country";
                    command.ExecuteNonQuery();

                    command.CommandText =
                    "CREATE TABLE [dbo].[Person] (" +
                    "[Id]   INT NOT NULL," +
                    "[Name] NVARCHAR (255)   NOT NULL," +
                    "[Balance] FLOAT         NOT NULL," +
                    "[Age]  INT              NOT NULL," +
                    "[IsMan]  Bit              NOT NULL, " +
                    "[CountryId]  INT              NOT NULL, " +
                    "[Sex] INT NOT NULL);";
                    command.ExecuteNonQuery();

                    command.CommandText =
                    "CREATE TABLE [dbo].[Country] (" +
                    "[Id]   INT NOT NULL," +
                    "[Name] NVARCHAR (255)   NOT NULL);";
                    command.ExecuteNonQuery();

                    command.CommandText = "INSERT INTO [dbo].[Person] VALUES (1, 'Doron',1000, 29, 1,1, 1)";
                    command.ExecuteNonQuery();

                    command.CommandText = "INSERT INTO [dbo].[Country] VALUES (1, 'Israel')";
                    command.ExecuteNonQuery();
                }
            }             
        }

        [Test]
        public void Where()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                var query =
                    from p in connection.Queryable<Person>()
                    where p.Age > 13
                    select  p;

                var list = query.ToList();

                Assert.That(1 == list.Count);
            }
        }

        [Test]
        public void MultipleWheres()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                var query = connection.Queryable<Person>().Where(p => p.Age > 13).Where(p => p.IsMan);

                var list = query.ToList();

                Assert.That(1 == list.Count);
            }
        }

        [Test]
        public void WhereWithBool()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                var query =
                    from p in connection.Queryable<Person>()
                    where p.IsMan
                    select p;

                var list = query.ToList();

                Assert.That(1 == list.Count);
            }
        }

        [Test]
        public void WhereWithNotBool()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                var query =
                    from p in connection.Queryable<Person>()
                    where !p.IsMan
                    select p;

                var list = query.ToList();

                Assert.That(0 == list.Count);
            }
        }

        [Test]
        public void WhereWithBoolParam()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                var query =
                    from p in connection.Queryable<Person>()
                    where p.IsMan == true
                    select p;

                var list = query.ToList();

                Assert.That(1 == list.Count);
            }
        }

        [Test]
        public void NoWhere()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                var query =
                    from p in connection.Queryable<Person>()                    
                    select p;

                var list = query.ToList();

                Assert.That(1 == list.Count);
            }
        }

        [Test]
        public void Enum()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                var query =
                    from p in connection.Queryable<Person>()
                    where p.Sex == Sex.Male
                    select p;

                var list = query.ToList();

                Assert.That(1 == list.Count);
            }
        }

        [Test]
        public void OrderBy()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();

                command.CommandText = "INSERT INTO [dbo].[Person] VALUES (2, 'Doron',1000, 30, 1, 1)";
                command.ExecuteNonQuery();


                var query =
                    from p in connection.Queryable<Person>()
                    orderby p.Name, p.Age descending 
                    select new {p.Name, p.Age};

                var list = query.ToList();

                Assert.That(list.First().Age == 30);
                Assert.That(list.Last().Age == 29);
            } 
        }

        [Test]
        public void Sum()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();

                command.CommandText = "INSERT INTO [dbo].[Person] VALUES (2, 'Doron',1000, 30, 1, 1)";
                command.ExecuteNonQuery();


                var query =
                    from p in connection.Queryable<Person>()                    
                    select p.Age;
                

                Assert.That(59 == query.Sum());                
            }
        }

        [Test]
        public void Any()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();

                command.CommandText = "INSERT INTO [dbo].[Person] VALUES (2, 'Doron',1000, 30, 1, 1)";
                command.ExecuteNonQuery();


                var query =
                    from p in connection.Queryable<Person>()
                    select p.Age;


                Assert.That(query.Any());
            }
        }

        [Test]
        public void First()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();

                command.CommandText = "INSERT INTO [dbo].[Person] VALUES (2, 'Doron',1000, 30, 1, 1)";
                command.ExecuteNonQuery();

                var query =
                    from p in connection.Queryable<Person>()
                    orderby p.Age descending 
                    select p;

                Assert.That(30 == query.First().Age);
            }
        }

        [Test, ExpectedException]
        public void Single()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();

                command.CommandText = "INSERT INTO [dbo].[Person] VALUES (2, 'Doron',1000, 30, 1, 1)";
                command.ExecuteNonQuery();

                var query =
                    from p in connection.Queryable<Person>()
                    orderby p.Age descending
                    select p;

                Assert.That(30 == query.Single().Age);
            }
        }

        [Test]
        public void Join()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                var query =
                    from p in connection.Queryable<Person>()
                    join c in connection.Queryable<Country>() on p.CountryId equals c.Id
                    select new {p.Name, CountryName = c.Name};

                var list = query.ToList();

                Assert.That(1 == list.Count);
                Assert.That("Doron" == list.First().Name);
                Assert.That("Israel" == list.First().CountryName);
            }
        }

        [Test]
        public void OnlyName()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                var query =
                    from p in connection.Queryable<Person>()
                    select p.Name;

                var list = query.ToList();

                Assert.That(1 == list.Count);
                Assert.That("Doron" == list.First());
            }
        }

        [Test]
        public void AnonymousProjection()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                var query =
                    from p in connection.Queryable<Person>()
                    select new {Name2 = p.Name, p.Age};

                var list = query.ToList();

                Assert.That(1 == list.Count);
                Assert.That("Doron" == list.First().Name2);
            }
        }


        [Test]
        public void AnonymousProjectionWithArthimetic()
        {            
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                var query =
                    from p in connection.Queryable<Person>()
                    select new {Name2= p.Name, Age = p.Age + 1};

                var list = query.ToList();

                Assert.That(1 == list.Count);
                Assert.That("Doron" == list.First().Name2);
                Assert.That(30 == list.First().Age);
            }
        }



        class PersonProjection
        {
            public PersonProjection()
            {
                
            }

            public PersonProjection(string name2, int age)
            {
                Name2 = name2;
                Age = age;
            }

            public string Name2 { get; set; }
            public int Age { get; set; }
        }

        [Test]
        public void TypedProjection()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                var query =
                    from p in connection.Queryable<Person>()
                    select new PersonProjection { Name2 = p.Name, Age = p.Age };

                var list = query.ToList();

                Assert.That(1 == list.Count);
                Assert.That("Doron" == list.First().Name2);
            }
        }

        [Test]
        public void TypedProjectionCtor()
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                var query =
                    from p in connection.Queryable<Person>()
                    select new PersonProjection (p.Name, p.Age );

                var list = query.ToList();

                Assert.That(1 == list.Count);
                Assert.That("Doron" == list.First().Name2);
            }
        }


    }
}
