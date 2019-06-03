using System;
using System.Collections.Generic;

namespace Talys
{
    internal static class CollectionExtensions
    {
        public static void Add<T>(this HashSet<T> set, IEnumerable<T> items)
        {
            set.UnionWith(items);
        }

        public static void ReplaceWith<T>(this HashSet<T> set, IEnumerable<T> items)
        {
            set.Clear();
            set.UnionWith(items);
        }
    }
}
