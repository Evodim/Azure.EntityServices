using Azure.EntityServices.Tables.Extensions;
using System;
using System.Globalization;

namespace Azure.EntityServices.Tables.Helpers
{
    internal static class Argument
    {
        /// <summary>
        /// Throw a <see cref="ArgumentNullException"/> if <paramref name="paramValue"/> is null.
        /// </summary>
        /// <typeparam name="T">The type of <paramref name="paramValue"/>.</typeparam>
        /// <param name="paramValue">The value parameter.</param>
        /// <param name="paramName">The name of the parameter.</param>
        public static void AssertNotNull<T>(T paramValue, string paramName = null)
        {
            if (paramValue == null)
                throw new ArgumentNullException(paramName);
        }

        /// <summary>
        /// Throw a <see cref="ArgumentException"/> if <paramref name="paramValue"/> is null or empty.
        /// For Non-Nullable types, check if <paramref name="paramValue"/> equals the type default value.
        /// For Nullable types, check if <paramref name="paramValue"/>.Value equals the type default value.
        /// </summary>
        /// <typeparam name="T">The type of <paramref name="paramValue"/>.</typeparam>
        /// <param name="paramValue">The value parameter.</param>
        /// <param name="paramName">The name of the parameter.</param>
        public static void AssertNotNullOrEmpty<T>(T paramValue, string paramName = null)
        {
            if (!paramValue.IsNullOrEmpty()) return;
            if (paramName == null) throw new ArgumentException("The argument must not be empty.");
            throw new ArgumentException("The argument must not be empty.", paramName);
        }

        /// <summary>
        /// Throw a <see cref="ArgumentException"/> if <paramref name="paramValue"/> is null or empty.
        /// For Non-Nullable types, check if <paramref name="paramValue"/> equals the type default value.
        /// For Nullable types, check if <paramref name="paramValue"/>.Value equals the type default value.
        /// </summary>
        /// <typeparam name="T">The type of <paramref name="paramValue"/>.</typeparam>
        /// <param name="paramValue">The value parameter.</param>
        /// <param name="paramName">The name of the parameter.</param>
        public static void AssertNotNullOrEmpty<T>(T? paramValue, string paramName = null) where T : struct
        {
            if (!paramValue.IsNullOrEmpty()) return;
            if (paramName == null) throw new ArgumentException("The argument must not be empty.");
            throw new ArgumentException("The argument must not be empty.", paramName);
        }

        /// <summary>
        /// Throw a <see cref="ArgumentOutOfRangeException"/> if <paramref name="paramValue"/> is not greater than <paramref name="minValue"/>.
        /// </summary>
        /// <typeparam name="T">The type of <paramref name="paramValue"/>.</typeparam>
        /// <param name="paramValue">The value parameter.</param>
        /// <param name="minValue">The min value to compare to <paramref name="paramValue"/>.</param>
        /// <param name="paramName">The name of the parameter.</param>
        public static void AssertGreaterThan<T>(T paramValue, T minValue, string paramName = null)
            where T : IComparable<T>
        {
            if (paramValue.IsGreaterThan(minValue)) return;

            if (paramName == null)
                throw new ArgumentOutOfRangeException(string.Format(CultureInfo.InvariantCulture,
                    "The argument is smaller than/equal to minimum of '{0}'", minValue));

            throw new ArgumentOutOfRangeException(paramName,
                string.Format(CultureInfo.InvariantCulture,
                    "The argument '{0}' is smaller than/equal to minimum of '{1}'", paramName, minValue));
        }

        /// <summary>
        /// Throw a <see cref="ArgumentOutOfRangeException"/> if <paramref name="paramValue"/> is less than <paramref name="minValue"/>.
        /// </summary>
        /// <typeparam name="T">The type of <paramref name="paramValue"/>.</typeparam>
        /// <param name="paramValue">The value parameter.</param>
        /// <param name="minValue">The min value to compare to <paramref name="paramValue"/>.</param>
        /// <param name="paramName">The name of the parameter.</param>
        public static void AssertGreaterThanOrEquals<T>(T paramValue, T minValue, string paramName = null) where T : IComparable<T>
        {
            if (paramValue.IsGreaterThanOrEquals(minValue)) return;

            if (paramName == null)
                throw new ArgumentOutOfRangeException(string.Format(CultureInfo.InvariantCulture,
                    "The argument is smaller than minimum of '{0}'", minValue));

            throw new ArgumentOutOfRangeException(paramName,
                    string.Format(CultureInfo.InvariantCulture,
                        "The argument '{0}' is smaller than minimum of '{1}'", paramName, minValue));
        }

        /// <summary>
        /// Throw a <see cref="ArgumentOutOfRangeException"/> if <paramref name="paramValue"/> is not less than <paramref name="maxValue"/>.
        /// </summary>
        /// <typeparam name="T">The type of <paramref name="paramValue"/>.</typeparam>
        /// <param name="paramValue">The value parameter.</param>
        /// <param name="maxValue">The max value to compare to <paramref name="paramValue"/>.</param>
        /// <param name="paramName">The name of the parameter.</param>
        public static void AssertLessThan<T>(T paramValue, T maxValue, string paramName = null) where T : IComparable<T>
        {
            if (paramValue.IsLessThan(maxValue)) return;

            if (paramName == null)
                throw new ArgumentOutOfRangeException(string.Format(CultureInfo.InvariantCulture,
                    "The argument is larger than/equal to maximum of '{10'", maxValue));

            throw new ArgumentOutOfRangeException(paramName,
                    string.Format(CultureInfo.InvariantCulture,
                        "The argument '{0}' is larger than/equal to maximum of '{1}'", paramName, maxValue));
        }

        /// <summary>
        /// Throw a <see cref="ArgumentOutOfRangeException"/> if <paramref name="paramValue"/> is greater than <paramref name="maxValue"/>.
        /// </summary>
        /// <typeparam name="T">The type of <paramref name="paramValue"/>.</typeparam>
        /// <param name="paramValue">The value parameter.</param>
        /// <param name="maxValue">The max value to compare to <paramref name="paramValue"/>.</param>
        /// <param name="paramName">The name of the parameter.</param>
        public static void AssertLessThanOrEquals<T>(T paramValue, T maxValue, string paramName = null)
            where T : IComparable<T>
        {
            if (paramValue.IsLessThanOrEquals(maxValue)) return;

            if (paramName == null)
                throw new ArgumentOutOfRangeException(string.Format(CultureInfo.InvariantCulture,
                    "The argument is larger than maximum of '{0}'", maxValue));

            throw new ArgumentOutOfRangeException(paramName,
                string.Format(CultureInfo.InvariantCulture,
                    "The argument '{0}' is larger than maximum of '{1}'", paramName, maxValue));
        }

        /// <summary>
        /// Throw a <see cref="ArgumentOutOfRangeException"/> if <paramref name="paramValue"/> is greater than <paramref name="maxValue"/> or less than <paramref name="minValue"/>.
        /// </summary>
        /// <typeparam name="T">The type of <paramref name="paramValue"/>.</typeparam>
        /// <param name="paramValue">The value parameter.</param>
        /// <param name="maxValue">The min value to compare to <paramref name="paramValue"/>.</param>
        /// <param name="minValue">The min value to compare to <paramref name="paramValue"/>.</param>
        /// <param name="paramName">The name of the parameter.</param>
        public static void AssertInRange<T>(T paramValue, T minValue, T maxValue, string paramName = null) where T : IComparable<T>
        {
            AssertGreaterThanOrEquals(paramValue, minValue, paramName);
            AssertLessThanOrEquals(paramValue, maxValue, paramName);
        }
    }
}