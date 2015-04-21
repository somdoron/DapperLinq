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
using Remotion.Linq.Clauses.ResultOperators;
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

        public static void AppendEnumerable(this StringBuilder stringBuilder, IEnumerable<string> e)
        {
            foreach (var item in e)
            {
                stringBuilder.Append(item);
            }
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
                    stringBuilder.AppendFormat("{0}{1}", prefix, item);
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
                var result = ExecuteCollection<T>(queryModel);

                return returnDefaultWhenEmpty ? result.SingleOrDefault() : result.Single();
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
            private List<string> m_orderByParts;
            private List<string> m_joinParts;

            public SqlQueryModelVisitor()
            {
                m_parameters = new Parameters();
                m_whereParts = new List<string>();
                m_orderByParts = new List<string>();
                m_joinParts = new List<string>();
            }

            public string GetSql()
            {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.AppendFormat("select {0}", m_selectPart);
                stringBuilder.AppendFormat(" from {0}", m_fromPart);
                stringBuilder.AppendEnumerable(m_joinParts);
                stringBuilder.AppendEnumerable(m_whereParts, " where ", " AND ");
                stringBuilder.AppendEnumerable(m_orderByParts, " order by ", ", ");

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

            public override void VisitResultOperator(ResultOperatorBase resultOperator, QueryModel queryModel, int index)
            {
                if (resultOperator is SumResultOperator)
                {
                    m_selectPart = string.Format("SUM({0})", m_selectPart);
                }
                else if (resultOperator is CountResultOperator)
                {
                    m_selectPart = string.Format("COUNT({0})", m_selectPart);
                }
                else if (resultOperator is AnyResultOperator)
                {
                    m_selectPart = string.Format("CASE COUNT({0}) WHEN 0 THEN 0 ELSE 1 END", m_selectPart);
                }
                else if (resultOperator is AverageResultOperator)
                {
                    m_selectPart = string.Format("AVG({0})", m_selectPart);
                }
                else if (resultOperator is MinResultOperator)
                {
                    m_selectPart = string.Format("MIN({0})", m_selectPart);
                }
                else if (resultOperator is MaxResultOperator)
                {
                    m_selectPart = string.Format("MAX({0})", m_selectPart);
                }
                else if (resultOperator is FirstResultOperator)
                {
                    m_selectPart = string.Format("TOP(1) {0}", m_selectPart);
                }
                else if (resultOperator is SingleResultOperator)
                {
                    // if we get more then one we throw exception
                    m_selectPart = string.Format("TOP(2) {0}", m_selectPart);
                }
                else if (resultOperator is LastResultOperator)
                {
                    throw new NotSupportedException("Last is not supported, reverse the order and use First");
                }
                else
                {
                    throw new NotSupportedException(resultOperator.GetType().Name + " is not supported");
                }

                base.VisitResultOperator(resultOperator, queryModel, index);
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

            public override void VisitOrderByClause(OrderByClause orderByClause, QueryModel queryModel, int index)
            {
                foreach (var ordering in orderByClause.Orderings)
                {
                    m_orderByParts.Add(OrderByExpressionVisitor.GetStatement(m_parameters, ordering.Expression, ordering.OrderingDirection));
                }
            }

            public override void VisitJoinClause(JoinClause joinClause, QueryModel queryModel, int index)
            {
                SqlExpressionVisitor sqlExpressionVisitor = new SqlExpressionVisitor(m_parameters);
                sqlExpressionVisitor.VisitExpression(joinClause.InnerKeySelector);
                string innerKey = sqlExpressionVisitor.GetStatement();

                sqlExpressionVisitor = new SqlExpressionVisitor(m_parameters);
                sqlExpressionVisitor.VisitExpression(joinClause.OuterKeySelector);
                string outerKey = sqlExpressionVisitor.GetStatement();

                m_joinParts.Add(string.Format(" JOIN {0} AS {1} ON {2} = {3}", joinClause.ItemType.Name, joinClause.ItemName, outerKey,
                    innerKey));

                base.VisitJoinClause(joinClause, queryModel, index);
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

            public SelectExpressionVisitor(Parameters parameters)
                : base(parameters)
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

            protected override Expression VisitMemberInitExpression(MemberInitExpression expression)
            {
                for (int i = 0; i < expression.Bindings.Count; i++)
                {
                    var binding = expression.Bindings[i] as MemberAssignment;

                    if (binding == null)
                    {
                        base.VisitMemberInitExpression(expression);
                        return expression;
                    }

                    if (i != 0)
                    {
                        Statement.Append(", ");
                    }

                    VisitExpression(binding.Expression);

                    Statement.AppendFormat(" AS {0}", binding.Member.Name);
                }

                return expression;
            }

            protected override Expression VisitNewExpression(NewExpression expression)
            {
                var parameters = expression.Constructor.GetParameters();

                for (int i = 0; i < expression.Arguments.Count; i++)
                {
                    if (i != 0)
                        Statement.Append(", ");

                    VisitExpression(expression.Arguments[i]);

                    Statement.AppendFormat(" AS {0}", parameters[i].Name);
                }

                return expression;
            }

            protected override Expression VisitArithmeticMemberExpression(MemberExpression expression)
            {
                return VisitMemberExpression(expression);
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

        class OrderByExpressionVisitor : SqlExpressionVisitor
        {
            public OrderByExpressionVisitor(Parameters parameters)
                : base(parameters)
            {
            }

            public static string GetStatement(Parameters parameters, Expression expression, OrderingDirection orderingDirection)
            {
                var expressionVisitor = new OrderByExpressionVisitor(parameters);
                expressionVisitor.VisitExpression(expression);

                if (orderingDirection == OrderingDirection.Desc)
                {
                    expressionVisitor.Statement.Append(" desc");
                }

                return expressionVisitor.GetStatement();
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
                expressionVisitor.Statement.Append("(");
                expressionVisitor.VisitExpression(expression);
                expressionVisitor.Statement.Append(")");
                return expressionVisitor.GetStatement();
            }
        }

        private class SqlExpressionVisitor : ThrowingExpressionTreeVisitor
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

            public override Expression VisitExpression(Expression expression)
            {
                return base.VisitExpression(expression);
            }

            protected override Expression VisitBinaryExpression(BinaryExpression expression)
            {
                string op = "";
                bool arthemetic;

                switch (expression.NodeType)
                {
                    case ExpressionType.Equal:
                        op = " = ";
                        arthemetic = true;
                        break;
                    case ExpressionType.NotEqual:
                        op = " <> ";
                        arthemetic = true;
                        break;
                    case ExpressionType.GreaterThan:
                        op = " > ";
                        arthemetic = true;
                        break;
                    case ExpressionType.GreaterThanOrEqual:
                        op = " >= ";
                        arthemetic = true;
                        break;
                    case ExpressionType.LessThan:
                        op = " < ";
                        arthemetic = true;
                        break;
                    case ExpressionType.LessThanOrEqual:
                        op = " <= ";
                        arthemetic = true;
                        break;
                    case ExpressionType.Add:
                        op = " + ";
                        arthemetic = true;
                        break;
                    case ExpressionType.Subtract:
                        op = " - ";
                        arthemetic = true;
                        break;
                    case ExpressionType.Multiply:
                        op = " * ";
                        arthemetic = true;
                        break;
                    case ExpressionType.Divide:
                        op = " / ";
                        arthemetic = true;
                        break;
                    case ExpressionType.AndAlso:
                        op = " AND ";
                        arthemetic = false;
                        break;
                    case ExpressionType.OrElse:
                        op = " OR ";
                        arthemetic = false;
                        break;
                    default:
                        base.VisitBinaryExpression(expression);
                        arthemetic = false;
                        break;
                }

                Statement.Append("(");

                if (arthemetic)
                {
                    if (expression.Left.NodeType == ExpressionType.MemberAccess)
                    {
                        VisitArithmeticMemberExpression(expression.Left as MemberExpression);
                    }
                    else
                    {
                        VisitExpression(expression.Left);
                    }

                    Statement.Append(op);

                    if (expression.Right.NodeType == ExpressionType.MemberAccess)
                    {
                        VisitArithmeticMemberExpression(expression.Right as MemberExpression);
                    }
                    else
                    {
                        VisitExpression(expression.Right);
                    }
                }
                else
                {
                    VisitExpression(expression.Left);
                    Statement.Append(op);
                    VisitExpression(expression.Right);
                }

                Statement.Append(")");

                return expression;
            }

            protected override Expression VisitUnaryExpression(UnaryExpression expression)
            {
                if (expression.NodeType == ExpressionType.Not)
                {
                    Statement.Append("NOT (");
                    VisitExpression(expression.Operand);
                    Statement.Append(")");
                }
                else if (expression.NodeType == ExpressionType.Convert)
                {
                    VisitExpression(expression.Operand);
                }
                else
                {
                    base.VisitUnaryExpression(expression);
                }

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

                if (expression.Type == typeof(bool))
                {
                    Statement.Append(" = 1");
                }

                return expression;
            }

            protected virtual Expression VisitArithmeticMemberExpression(MemberExpression expression)
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
