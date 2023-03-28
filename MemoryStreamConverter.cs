#nullable enable

namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.IO;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using System.Text.Json.Serialization;

    sealed class MemoryStreamConverter : JsonConverter<MemoryStream>
    {
        public const string PropertyName = "MemoryStreamContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

        public override MemoryStream Read(ref Utf8JsonReader reader, Type typeToConvert,
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
            if (reader.TokenType != JsonTokenType.String)
            {
                throw new JsonException();
            }
            var stream = new MemoryStream(reader.GetBytesFromBase64());

            reader.Read();

            if (reader.TokenType != JsonTokenType.EndObject)
            {
                throw new JsonException();
            }
            return stream;
        }

        public override void Write(Utf8JsonWriter writer, MemoryStream value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteBase64String(PropertyName, value.ToArray());
            writer.WriteEndObject();
        }

        public static bool TryExtract(JsonProperty property, out MemoryStream? memoryStream)
        {
            memoryStream = null;
            if (!property.NameEquals(PropertyName))
            {
                return false;
            }
            memoryStream = new MemoryStream(property.Value.GetBytesFromBase64());
            return true;
        }

        public static bool TryConvert(MemoryStream? memoryStream, out JsonObject? jsonObject)
        {
            jsonObject = null;
            if (memoryStream is null)
            {
                return false;
            }

            jsonObject = new JsonObject
            {
                [PropertyName] = Convert.ToBase64String(memoryStream.ToArray())
            };
            return true;
        }
    }
}