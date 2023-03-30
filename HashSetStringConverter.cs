namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Text.Json;
    using System.Text.Json.Serialization;

    sealed class HashSetStringConverter : JsonConverterFactory
    {
        // This is a cryptic property name to make sure we never class with the user data
        const string PropertyName = "HashSetStringContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

        public override bool CanConvert(Type typeToConvert)
            => typeToConvert.IsGenericType && typeof(ISet<string>).IsAssignableFrom(typeToConvert);

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            var converter = (JsonConverter)Activator.CreateInstance(
                typeof(SetConverter<>)
                    .MakeGenericType(new Type[] { typeToConvert }),
                BindingFlags.Instance | BindingFlags.Public,
                binder: null,
                args: null,
                culture: null)!;
            return converter;
        }

        sealed class SetConverter<TSet> : JsonConverter<TSet> where TSet : ISet<string>
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

        public static bool TryExtract(JsonProperty property, out List<string?>? strings)
        {
            strings = null;
            if (!property.NameEquals(PropertyName))
            {
                return false;
            }

            foreach (var innerElement in property.Value.EnumerateArray())
            {
                strings ??= new List<string?>(property.Value.GetArrayLength());
                strings.Add(innerElement.GetString());
            }

            strings ??= new List<string?>(0);
            return true;
        }
    }
}