using Sitecore;
using Sitecore.Configuration;
using Sitecore.Data;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Data.Eventing;
using Sitecore.Diagnostics;
using Sitecore.Eventing;
using Sitecore.Reflection;
using System;
using System.Linq;
using System.Reflection;

namespace Sitecore.Support.Tasks
{
  [UsedImplicitly]
  public class CleanupEventQueue
  {
    private SqlDataApi dataApi;
    private readonly int minutesToKeep;

    public CleanupEventQueue(string minutesToKeep)
    {
      Assert.ArgumentNotNull(minutesToKeep, "minutesToKeep");
      this.minutesToKeep = int.Parse(minutesToKeep);
    }

    private void Cleanup(Database database, DateTime toUtcDate)
    {
      Assert.ArgumentNotNull(database, "database");
      EventQueueQuery query = new EventQueueQuery
      {
        ToUtcDate = new DateTime?(toUtcDate)
      };
      SqlStatement sqlStatement = this.GetSqlStatement(database, query);
      Assert.IsNotNull(sqlStatement, "sqlStatement");
      sqlStatement.Select = "DELETE";
      sqlStatement.OrderBy = string.Empty;
      SqlDataApi dataApi = this.GetDataApi(database);
      Assert.IsNotNull(dataApi, "sqlDataApi: " + database.Name);
      Log.Info($"Cleaning up event queue of the {database.Name} database to keep only the records created after {toUtcDate}", this);
      dataApi.Execute(sqlStatement.Sql, sqlStatement.GetParameters());
    }

    private SqlDataApi GetDataApi(Database database)
    {
      Assert.ArgumentNotNull(database, "database");
      return (SqlDataApi)ReflectionUtil.GetProperty(database.RemoteEvents.Queue, "DataApi");
    }

    private SqlStatement GetSqlStatement(Database database, EventQueueQuery query)
    {
      Assert.ArgumentNotNull(database, "database");
      Assert.ArgumentNotNull(query, "query");
      MethodInfo method = typeof(SqlEventQueue).GetMethod("GetSqlStatement", BindingFlags.NonPublic | BindingFlags.Instance);
      Assert.IsNotNull(method, "The GetSqlStatement method is not found in SqlEventQueue which is most likely caused by different Sitecore version rather it was created for (7.1 rev. 140130)");
      return (SqlStatement)method.Invoke(database.RemoteEvents.Queue, new object[] { query });
    }

    [UsedImplicitly]
    public void Run()
    {
      DateTime toUtcDate = DateTime.UtcNow.AddMinutes((double)-this.minutesToKeep);
      foreach (Database database in Factory.GetDatabases().Where(database => database.RemoteEvents.Queue is SqlEventQueue))
      {
        this.Cleanup(database, toUtcDate);
      }
    }
  }
}
