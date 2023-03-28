#nullable enable
namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using System.Text.Json.Serialization;

    sealed class HashSetStringConverter : JsonConverterFactory
    {
        public const string PropertyName = "HashSetStringContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

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
            public override TSet? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType != JsonTokenType.StartObject)
                {
                    throw new JsonException();
                }

                reader.Read();
                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    throw new JsonException();
                }

                string? propertyName = reader.GetString();
                if (propertyName != PropertyName)
                {
                    throw new JsonException();
                }

                reader.Read();
                if (reader.TokenType != JsonTokenType.StartArray)
                {
                    throw new JsonException();
                }

                // Deliberately not passing the options to use the default json serialization behavior
                var set = JsonSerializer.Deserialize<TSet>(ref reader);

                reader.Read();

                if (reader.TokenType != JsonTokenType.EndObject)
                {
                    throw new JsonException();
                }
                return set;
            }

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

        public static bool TryConvert(List<string> strings, out JsonObject? jsonObject)
        {
            jsonObject = null;
            if (strings is not { Count: > 0 })
            {
                return false;
            }

            jsonObject = new JsonObject();
            var stringHashSetContent = new JsonArray();
            foreach (var stringValue in strings)
            {
                stringHashSetContent.Add(stringValue);
            }
            jsonObject.Add(PropertyName, stringHashSetContent);
            return true;
        }
    }
}