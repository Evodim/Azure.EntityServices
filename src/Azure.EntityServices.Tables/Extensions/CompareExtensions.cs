using Azure.EntityServices.Tables.Helpers;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Azure.EntityServices.Tables.Extensions
{
    internal static class CompareExtensions
    {
        /// <summary>
        /// Check if the specified <paramref name="value"/> is null or empty.
        /// </summary>
        /// <typeparam name="T">The element type of the <paramref name="value"/>.</typeparam>
        /// <param name="value">The value to check.</param>
        /// <returns>true if the <paramref name="value"/> is null or empty; otherwise, false.</returns>
        public static bool IsNullOrEmpty<T>(this T value)
        {
            if (value == null) return true;

            return value is IEnumerable collection ? CollectionIsEmpty(collection) : IsDefaultValue(value);
        }

        /// <summary>
        /// Check if the specified <paramref name="value"/> is null or empty.
        /// </summary>
        /// <typeparam name="T">The element type of the <paramref name="value"/>.</typeparam>
        /// <param name="value">The value to check.</param>
        /// <returns>true if the <paramref name="value"/> is null or empty; otherwise, false.</returns>
        public static bool IsNullOrEmpty<T>(this T? value) where T : struct
        {
            return !value.HasValue || IsDefaultValue(value.Value);
        }

        /// <summary>
        /// Check if the specified <paramref name="value"/> is null or empty.
        /// </summary>
        /// <param name="value">The value to check.</param>
        /// <returns>true if the <paramref name="value"/> is null or empty; otherwise, false.</returns>
        public static bool IsNullOrEmpty(this string value)
        {
            return string.IsNullOrEmpty(value);
        }

        /// <summary>
        /// Check if the specified <paramref name="value"/> is greather than <paramref name="min"/>.
        /// </summary>
        /// <typeparam name="T">The element type of the values to compare.</typeparam>
        /// <param name="value">The value to check.</param>
        /// <param name="min">The min value.</param>
        /// <returns>true if the <paramref name="value"/> is greather than <paramref name="min"/>; otherwise, false.</returns>
        public static bool IsGreaterThan<T>(this T value, T min) where T : IComparable<T>
        {
            Argument.AssertNotNull(value, nameof(value));
            return value.CompareTo(min) > 0;
        }

        /// <summary>
        /// Check if the specified <paramref name="value"/> is less than <paramref name="max"/>.
        /// </summary>
        /// <typeparam name="T">The element type of the values to compare.</typeparam>
        /// <param name="value">The value to check.</param>
        /// <param name="max">The max value.</param>
        /// <returns>true if the <paramref name="value"/> is less than <paramref name="max"/>; otherwise, false.</returns>
        public static bool IsLessThan<T>(this T value, T max) where T : IComparable<T>
        {
            Argument.AssertNotNull(value, nameof(value));
            return value.CompareTo(max) < 0;
        }

        /// <summary>
        /// Check if the specified <paramref name="value"/> is equal to <paramref name="other"/>.
        /// </summary>
        /// <typeparam name="T">The element type of the values to compare.</typeparam>
        /// <param name="value">The value to check.</param>
        /// <param name="other">The other value.</param>
        /// <returns>true if the <paramref name="value"/> is equals to <paramref name="other"/>; otherwise, false.</returns>
        public static bool IsEqualTo<T>(this T value, T other) where T : IComparable<T>
        {
            Argument.AssertNotNull(value, nameof(value));
            return value.CompareTo(other) == 0;
        }

        /// <summary>
        /// Check if the specified <paramref name="value"/> is greather than/equals to <paramref name="min"/>.
        /// </summary>
        /// <typeparam name="T">The element type of the values to compare.</typeparam>
        /// <param name="value">The value to check.</param>
        /// <param name="min">The min value.</param>
        /// <returns>true if the <paramref name="value"/> is greather than/equals to <paramref name="min"/>; otherwise, false.</returns>
        public static bool IsGreaterThanOrEquals<T>(this T value, T min) where T : IComparable<T>
        {
            return !value.IsLessThan(min);
        }

        /// <summary>
        /// Check if the specified <paramref name="value"/> is less than/equals to <paramref name="max"/>.
        /// </summary>
        /// <typeparam name="T">The element type of the values to compare.</typeparam>
        /// <param name="value">The value to check.</param>
        /// <param name="max">The max value.</param>
        /// <returns>true if the <paramref name="value"/> is less than/equals to <paramref name="max"/>; otherwise, false.</returns>
        public static bool IsLessThanOrEquals<T>(this T value, T max) where T : IComparable<T>
        {
            return !value.IsGreaterThan(max);
        }

        /// <summary>
        /// Check if the specified <paramref name="value"/> is greather than/equals to <paramref name="min"/> and less than/equals to <paramref name="max"/>.
        /// </summary>
        /// <typeparam name="T">The element type of the values to compare.</typeparam>
        /// <param name="value">The value to check.</param>
        /// <param name="min">The min value.</param>
        /// <param name="max">The max value.</param>
        /// <returns>true if the <paramref name="value"/> is greather than/equals to <paramref name="min"/> and less than/equals to <paramref name="max"/>; otherwise, false.</returns>
        public static bool IsInRange<T>(this T value, T min, T max) where T : IComparable<T>
        {
            Argument.AssertGreaterThanOrEquals(max, min, nameof(max));
            return !value.IsLessThan(min) && !value.IsGreaterThan(max);
        }

        /// <summary>
        /// Check if the specified <paramref name="value"/> equals the default value.
        /// </summary>
        /// <typeparam name="T">The element type of the <paramref name="value"/>.</typeparam>
        /// <param name="value">The value to check.</param>
        /// <returns>true if the <paramref name="value"/> equals the default value; otherwise, false.</returns>
        public static bool IsDefaultValue<T>(this T value)
        {
            return EqualityComparer<T>.Default.Equals(value, default);
        }

        /// <summary>
        /// Check is the non-generic collection in parameter is empty.
        /// </summary>
        /// <param name="collection">The non-generic collection to check.</param>
        /// <returns>true if the collection is empty; otherwise, false.</returns>
        private static bool CollectionIsEmpty(IEnumerable collection)
        {
            // ReSharper disable once LoopCanBeConvertedToQuery
            // Don't want to perform an unnecesary cast
            foreach (var _ in collection)
            {
                return false;
            }

            return true;
        }
    }
}