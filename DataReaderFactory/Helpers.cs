namespace DataReaderFactory
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq.Expressions;
    using System.Threading;

    internal static class Helpers
    {
        internal static ParameterExpression ObjArrayParam { get; } = Expression.Parameter(typeof(object[]), "data");
        internal static ParameterExpression StateParam { get; } = Expression.Parameter(typeof(object), "state");

        internal static T EnsurePopulated<T, TOther>(ref T target, Func<TOther, T> valueFactory, TOther state) where T:class
        => Volatile.Read(ref target!) ?? EnsurePopulatedCore(ref target, valueFactory, state);

        private static T EnsurePopulatedCore<T, TOther>(ref T target, Func<TOther, T> valueFactory, TOther state) where T:class
        {
            Interlocked.CompareExchange(ref target, valueFactory(state), null!);
            return target;
        }
    }
}
