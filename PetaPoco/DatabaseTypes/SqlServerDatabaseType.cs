// PetaPoco - A Tiny ORMish thing for your POCO's.
// Copyright © 2011-2012 Topten Software.  All Rights Reserved.

using System;
using System.Linq;
using System.Text.RegularExpressions;
using PetaPoco.Internal;

namespace PetaPoco.DatabaseTypes
{
	class SqlServerDatabaseType : DatabaseType
	{
		public override string BuildPageQuery(long skip, long take, PagingHelper.SQLParts parts, ref object[] args)
		{
			string query = parts.sqlSelectRemoved;
			if (!string.IsNullOrWhiteSpace(parts.sqlOrderBy))
				query = query.Replace(parts.sqlOrderBy, string.Empty);

			if (PagingHelper.rxDistinct.IsMatch(query))
			{
				query = "peta_inner.* FROM (SELECT " + query + ") peta_inner";
			}
			var sqlPage = string.Format("SELECT * FROM (SELECT ROW_NUMBER() OVER ({0}) peta_rn, {1}) peta_paged WHERE peta_rn>@{2} AND peta_rn<=@{3}",
									parts.sqlOrderBy == null ? "ORDER BY (SELECT NULL)" : parts.sqlOrderBy, query, args.Length, args.Length + 1);
			args = args.Concat(new object[] { skip, skip + take }).ToArray();

			return sqlPage;
		}

		public override object ExecuteInsert(Database db, System.Data.IDbCommand cmd, string PrimaryKeyName)
		{
			return db.ExecuteScalarHelper(cmd);
		}

		public override string GetExistsSql()
		{
			return "IF EXISTS (SELECT 1 FROM {0} WHERE {1}) SELECT 1 ELSE SELECT 0";
		}

		public override string GetInsertOutputClause(string primaryKeyName)
		{
			return String.Format(" OUTPUT INSERTED.[{0}]", primaryKeyName);
		}
	}

}
