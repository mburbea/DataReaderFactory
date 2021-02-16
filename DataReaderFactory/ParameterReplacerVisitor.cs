using System.Linq;
using System.Linq.Expressions;

namespace DataReaderFactory
{
    internal sealed class ParameterReplacerVistor : ExpressionVisitor
    {
        private readonly ParameterExpression[] _paramExpressions;

        public ParameterReplacerVistor(params ParameterExpression[] paramExpressions) => _paramExpressions = paramExpressions;

        protected override Expression VisitParameter(ParameterExpression node) => _paramExpressions.FirstOrDefault(param => param.Type == node.Type) ?? node;
    }
}
