using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Azure.EntityServices.Tables.Extensions
{
    internal static class ExpressionExtensions
    {
        public static PropertyInfo GetPropertyInfo<T, U>(this Expression<Func<T, U>> expression)
        {
            if (expression.Body is not MemberExpression member)
            {
                // The property access might be getting converted to object to match the func
                // If so, get the operand and see if that's a member expression
                member = (expression.Body as UnaryExpression)?.Operand as MemberExpression;
            }
            if (member == null)
            {
                //Action must be a member expression
                return null;
            }
            if (member.Member is PropertyInfo)
                return member.Member as PropertyInfo;

            //Expression member is not a property
            return null;
        }
    }
}