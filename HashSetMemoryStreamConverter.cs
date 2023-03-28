#nullable enable
namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using System.Text.Json.Serialization;

    sealed class HashSetMemoryStreamConverter : JsonConverterFactory
    {
        public const string PropertyName = "HashSetMemoryStreamContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

        public override bool CanConvert(Type typeToConvert)
            => typeToConvert.IsGenericType && typeof(ISet<MemoryStream>).IsAssignableFrom(typeToConvert);

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            var converter = (JsonConverter)Activator.CreateInstance(
                typeof(SetConverter<>)
                    .MakeGenericType(new Type[] { typeToConvert }),
                BindingFlags.Instance | BindingFlags.Public,
                binder: null,
                args: new[] { options },
                culture: null)!;
            return converter;
        }

        sealed class SetConverter<TSet> : JsonConverter<TSet> where TSet : ISet<MemoryStream>
        {
            public SetConverter(JsonSerializerOptions options)
            {
                var streamConverter = (JsonConverter<MemoryStream>)options.GetConverter(typeof(MemoryStream));
                memoryStreamOptions = new JsonSerializerOptions { Converters = { streamConverter } };
            }

            public override TSet? Read(ref Utf8JsonReader reader, Type typeToConvert,
                JsonSerializerOptions options)
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

                var set = JsonSerializer.Deserialize<TSet>(ref reader, memoryStreamOptions);

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
                JsonSerializer.Serialize(writer, value, memoryStreamOptions);
                writer.WriteEndObject();
            }

            readonly JsonSerializerOptions memoryStreamOptions;
        }

        public static bool TryExtract(JsonProperty property, out List<MemoryStream?>? memoryStreams)
        {
            memoryStreams = null;
            if (!property.NameEquals(PropertyName))
            {
                return false;
            }

            foreach (var innerElement in property.Value.EnumerateArray())
            {
                memoryStreams ??= new List<MemoryStream?>(property.Value.GetArrayLength());
                foreach (var streamElement in innerElement.EnumerateObject())
                {
                    memoryStreams.Add(new MemoryStream(streamElement.Value.GetBytesFromBase64()));
                }
            }

            memoryStreams ??= new List<MemoryStream?>(0);
            return true;
        }

        public static bool TryConvert(List<MemoryStream> memoryStreams, out JsonObject? jsonObject)
        {
            jsonObject = null;
            if (memoryStreams is not { Count: > 0 })
            {
                return false;
            }

            jsonObject = new JsonObject();
            var streamHashSetContent = new JsonArray();
            foreach (var memoryStream in memoryStreams)
            {
                _ = MemoryStreamConverter.TryConvert(memoryStream, out var memoryStreamJsonObject);
                streamHashSetContent.Add(memoryStreamJsonObject);
            }
            jsonObject.Add(PropertyName, streamHashSetContent);
            return true;
        }
    }
}