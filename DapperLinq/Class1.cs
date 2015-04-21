using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Clauses.ExpressionTreeVisitors;
using Remotion.Linq.Parsing;
using Remotion.Linq.Parsing.Structure;

namespace DapperLinq
{
    public static class DapperLinqMapper
    {
        public static IQueryable<T> Queryable<T>(this IDbConnection connection, IDbTransaction transaction = null)
        {
            return new SqlQueryable<T>(connection, transaction);
        }

        public static void AppendEnumerable(this StringBuilder stringBuilder, IEnumerable<string> e, string delimiter)
        {
            bool first = true;

            foreach (var item in e)
            {
                if (first)
                {
                    first = false;
                    stringBuilder.Append(item);
                }
                else
                {
                    stringBuilder.AppendFormat("{0}{1}", delimiter, item);
                }
            }
        }

        public static void AppendEnumerable(this StringBuilder stringBuilder, IEnumerable<string> e, string prefix, string delimiter)
        {
            bool first = true;

            foreach (var item in e)
            {
                if (first)
                {
                    first = false;
                    stringBuilder.AppendFormat("{0}{1}",prefix, item);
                }
                else
                {
                    stringBuilder.AppendFormat("{0}{1}", delimiter, item);
                }
            }
        }

        class SqlQueryable<T> : QueryableBase<T>
        {
            public SqlQueryable(IDbConnection dbConnection, IDbTransaction transaction)
                : base(QueryParser.CreateDefault(),
                    new SqlQueryExecuter(dbConnection, transaction))
            {

            }

            public SqlQueryable(IQueryProvider provider, Expression expression)
                : base(provider, expression)
            {
            }
        }

        class SqlQueryExecuter : IQueryExecutor
        {
            private readonly IDbConnection m_connection;
            private readonly IDbTransaction m_transaction;

            public SqlQueryExecuter(IDbConnection connection, IDbTransaction transaction)
            {
                m_connection = connection;
                m_transaction = transaction;
            }

            public T ExecuteScalar<T>(QueryModel queryModel)
            {
                SqlQueryModelVisitor visitor = new SqlQueryModelVisitor();
                visitor.VisitQueryModel(queryModel);
                string sql = visitor.GetSql();

                return m_connection.ExecuteScalar<T>(sql, visitor.Parameters, m_transaction);
            }

            public T ExecuteSingle<T>(QueryModel queryModel, bool returnDefaultWhenEmpty)
            {
                var result = ExecuteScalar<T>(queryModel);

                // Throw exception if empty

                return result;
            }

            public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel)
            {
                SqlQueryModelVisitor visitor = new SqlQueryModelVisitor();
                visitor.VisitQueryModel(queryModel);
                string sql = visitor.GetSql();

                return m_connection.Query<T>(sql, visitor.Parameters, m_transaction);
            }
        }

        class SqlQueryModelVisitor : QueryModelVisitorBase
        {
            private Parameters m_parameters;
            private string m_fromPart;
            private List<string> m_whereParts;
            private string m_selectPart;

            public SqlQueryModelVisitor()
            {
                m_parameters = new Parameters();
                m_whereParts = new List<string>();
            }

            public string GetSql()
            {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.AppendFormat("select {0}", m_selectPart);
                stringBuilder.AppendFormat(" from {0}", m_fromPart);
                stringBuilder.AppendEnumerable(m_whereParts, " where ", " AND ");

                return stringBuilder.ToString();
            }

            public Parameters Parameters
            {
                get { return m_parameters; }
            }

            public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel)
            {
                m_fromPart = string.Format("{0} as {1}", fromClause.ItemType.Name, fromClause.ItemName);

                base.VisitMainFromClause(fromClause, queryModel);
            }

            public override void VisitSelectClause(SelectClause selectClause, QueryModel queryModel)
            {
                m_selectPart = SelectExpressionVisitor.GetStatement(m_parameters, selectClause.Selector);

                base.VisitSelectClause(selectClause, queryModel);
            }

            public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
            {
                m_whereParts.Add(WhereExpressionVisitor.GetStatement(m_parameters, whereClause.Predicate));
            }
        }

        class Parameter
        {
            public Parameter(string name, object value)
            {
                Name = name;
                Value = value;
            }

            public string Name { get; private set; }
            public object Value { get; private set; }
        }

        class Parameters : SqlMapper.IDynamicParameters
        {
            private List<Parameter> m_parameters;

            public Parameters()
            {
                m_parameters = new List<Parameter>();
            }

            public Parameter AddParameter(object value)
            {
                Parameter parameter = new Parameter(string.Format("p{0}", m_parameters.Count), value);
                m_parameters.Add(parameter);
                return parameter;
            }

            void SqlMapper.IDynamicParameters.AddParameters(IDbCommand command, SqlMapper.Identity identity)
            {
                foreach (var parameter in m_parameters)
                {
                    var p = command.CreateParameter();
                    p.ParameterName = parameter.Name;
                    p.Value = parameter.Value ?? DBNull.Value;
                    command.Parameters.Add(p);
                }
            }
        }

        class SelectExpressionVisitor : SqlExpressionVisitor
        {
            private readonly Parameters m_parameters;
            
            public SelectExpressionVisitor(Parameters parameters) : base(parameters)
            {
                m_parameters = parameters;             
            }

            public static string GetStatement(Parameters parameters, Expression expression)
            {
                var expressionVisitor = new SelectExpressionVisitor(parameters);
                expressionVisitor.VisitExpression(expression);
                return expressionVisitor.GetStatement();
            }

            protected override Expression VisitQuerySourceReferenceExpression(QuerySourceReferenceExpression expression)
            {
                Statement.AppendEnumerable(expression.ReferencedQuerySource.ItemType.GetProperties().Select(p => p.Name), string.Format("{0}.",
                    expression.ReferencedQuerySource.ItemName), string.Format(", {0}.", expression.ReferencedQuerySource.ItemName));
                return expression;
            }

            protected override Expression VisitMemberExpression(MemberExpression expression)
            {
                if (expression.Expression.NodeType == QuerySourceReferenceExpression.ExpressionType)
                {
                    QuerySourceReferenceExpression querySourceReferenceExpression =
                        (QuerySourceReferenceExpression)expression.Expression;

                    Statement.Append(querySourceReferenceExpression.ReferencedQuerySource.ItemName);
                }
                else
                {
                    VisitExpression(expression.Expression);
                }

                Statement.AppendFormat(".{0}", expression.Member.Name);

                return expression;
            }
        }

        class WhereExpressionVisitor : SqlExpressionVisitor
        {
            private readonly Parameters m_parameters;
            
            public WhereExpressionVisitor(Parameters parameters)
                : base(parameters)
            {
                m_parameters = parameters;                
            }

            public static string GetStatement(Parameters parameters, Expression expression)
            {
                var expressionVisitor = new WhereExpressionVisitor(parameters);
                expressionVisitor.VisitExpression(expression);
                return expressionVisitor.GetStatement();
            }
        }

        class SqlExpressionVisitor : ThrowingExpressionTreeVisitor
        {
            private readonly Parameters m_parameters;

            public StringBuilder Statement { get; private set; }

            public SqlExpressionVisitor(Parameters parameters)
            {
                m_parameters = parameters;
                Statement = new StringBuilder();
            }      

            public string GetStatement()
            {                
                return Statement.ToString();
            }

            protected override Expression VisitBinaryExpression(BinaryExpression expression)
            {
                VisitExpression(expression.Left);

                switch (expression.NodeType)
                {
                    case ExpressionType.Equal:
                        Statement.Append(" = ");
                        break;
                    case ExpressionType.NotEqual:
                        Statement.Append(" <> ");
                        break;
                    case ExpressionType.GreaterThan:
                        Statement.Append(" > ");
                        break;
                    case ExpressionType.GreaterThanOrEqual:
                        Statement.Append(" >= ");
                        break;
                    case ExpressionType.LessThan:
                        Statement.Append(" < ");
                        break;
                    case ExpressionType.LessThanOrEqual:
                        Statement.Append(" <= ");
                        break;
                    case ExpressionType.Add:
                        Statement.Append(" + ");
                        break;
                    case ExpressionType.Subtract:
                        Statement.Append(" - ");
                        break;
                    case ExpressionType.Multiply:
                        Statement.Append(" * ");
                        break;
                    case ExpressionType.Divide:
                        Statement.Append(" / ");
                        break;
                    case ExpressionType.And:
                    case ExpressionType.AndAlso:
                        Statement.Append(" AND ");
                        break;
                    case ExpressionType.Or:
                    case ExpressionType.OrElse:
                        Statement.Append(" OR ");
                        break;                        
                    default:
                        base.VisitBinaryExpression(expression);
                        break;
                }

                VisitExpression(expression.Right);
                return expression;
            }

            protected override Expression VisitQuerySourceReferenceExpression(QuerySourceReferenceExpression expression)
            {
                Statement.Append(expression.ReferencedQuerySource.ItemName);
                return expression;
            }            

            protected override Expression VisitMemberExpression(MemberExpression expression)
            {
                VisitExpression(expression.Expression);
                Statement.AppendFormat(".{0}", expression.Member.Name);

                return expression;
            }

            protected override Expression VisitConstantExpression(ConstantExpression expression)
            {
                var namedParameter = m_parameters.AddParameter(expression.Value);
                Statement.AppendFormat("@{0}", namedParameter.Name);

                return expression;
            }

            protected override Exception CreateUnhandledItemException<T>(T unhandledItem, string visitMethod)
            {
                string itemText = FormatUnhandledItem(unhandledItem);
                var message = string.Format("The expression '{0}' (type: {1}) is not supported by this LINQ provider.", itemText, typeof(T));
                return new NotSupportedException(message);
            }

            private string FormatUnhandledItem<T>(T unhandledItem)
            {
                var itemAsExpression = unhandledItem as Expression;
                return itemAsExpression != null ? FormattingExpressionTreeVisitor.Format(itemAsExpression) : unhandledItem.ToString();
            }
        } 
    }
}
