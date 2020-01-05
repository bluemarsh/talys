using System;
using System.Collections;
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

        public static void ReplaceWith<TKey, TValue>(
            this Dictionary<TKey, TValue> map,
            IEnumerable<KeyValuePair<TKey, TValue>> items)
        {
            map.Clear();
            foreach (var pair in items)
                map.Add(pair.Key, pair.Value);
        }

        public static void OverrideWith<TKey, TValue>(
            this Dictionary<TKey, TValue> map,
            IEnumerable<KeyValuePair<TKey, TValue>> items)
        {
            foreach (var pair in items)
                map[pair.Key] = pair.Value;
        }

        public static void Deconstruct<TKey, TValue>(this KeyValuePair<TKey, TValue> pair, out TKey key, out TValue value)
        {
            key = pair.Key;
            value = pair.Value;
        }
    }
}
