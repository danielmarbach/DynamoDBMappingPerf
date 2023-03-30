namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Text.Json;
    using System.Text.Json.Serialization;

    sealed class HashSetOfNumberConverter : JsonConverterFactory
    {
        // This is a cryptic property name to make sure we never class with the user data
        const string PropertyName = "HashSetNumberContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

        public override bool CanConvert(Type typeToConvert)
        {
            if (!typeToConvert.IsGenericType)
            {
                return false;
            }

            var innerTypeToConvert = typeToConvert.GetGenericArguments()[0];
            var isNumberTypeSupported = innerTypeToConvert == typeof(byte) ||
                                        innerTypeToConvert == typeof(sbyte) ||
                                        innerTypeToConvert == typeof(ushort) ||
                                        innerTypeToConvert == typeof(uint) ||
                                        innerTypeToConvert == typeof(ulong) ||
                                        innerTypeToConvert == typeof(long) ||
                                        innerTypeToConvert == typeof(short) ||
                                        innerTypeToConvert == typeof(int) ||
                                        innerTypeToConvert == typeof(double) ||
                                        innerTypeToConvert == typeof(decimal) ||
                                        innerTypeToConvert == typeof(float);

            return isNumberTypeSupported &&
                   // This check is fairly expensive so we do it only once we know it is a number type that is supported
                   typeToConvert.IsAssignableToGenericType(typeof(ISet<>));
        }

        public override JsonConverter CreateConverter(Type type, JsonSerializerOptions options)
        {
            Type valueType = type.GetGenericArguments()[0];
            var converter = (JsonConverter)Activator.CreateInstance(
                typeof(SetConverter<,>)
                    .MakeGenericType(new Type[] { type, valueType }),
                BindingFlags.Instance | BindingFlags.Public,
                binder: null,
                args: null,
                culture: null)!;
            return converter;

        }
        sealed class SetConverter<TSet, TValue> : JsonConverter<TSet>
            where TSet : ISet<TValue>
            where TValue : struct
        {
            public override TSet? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
                throw new NotImplementedException(
                $"The {GetType().FullName} should never be used on the read path since its sole purpose is to preserve information on the write path");

            public override void Write(Utf8JsonWriter writer, TSet value, JsonSerializerOptions options)
            {
                writer.WriteStartObject();
                writer.WritePropertyName(PropertyName);
                // Deliberately not passing the options to use the default json serialization behavior
                JsonSerializer.Serialize(writer, value);
                writer.WriteEndObject();
            }
        }

        public static bool TryExtract(JsonProperty property, out List<string?>? numbersAsStrings)
        {
            numbersAsStrings = null;
            if (!property.NameEquals(PropertyName))
            {
                return false;
            }

            foreach (var innerElement in property.Value.EnumerateArray())
            {
                numbersAsStrings ??= new List<string?>(property.Value.GetArrayLength());
                numbersAsStrings.Add(innerElement.ToString());
            }
            numbersAsStrings ??= new List<string?>(0);
            return true;
        }
    }
}