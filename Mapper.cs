namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using System.Text.Json.Serialization;
    using Amazon.DynamoDBv2.Model;

    static class Mapper
    {
        static readonly JsonSerializerOptions classToMapSerializerOptions =
            new()
            {
                Converters =
                {
                    new MemoryStreamConverter(),
                    new HashSetMemoryStreamConverter(),
                    new HashSetStringConverter(),
                    new HashSetOfNumberConverter()
                }
            };

        static readonly JsonSerializerOptions mapToClassSerializerOptions =
            new()
            {
                Converters =
                {
                    new MemoryStreamConverter(),
                    new HashSetMemoryStreamConverter()
                },
                // DynamoDB returns us numbers as strings and we need to be able to read them
                NumberHandling = JsonNumberHandling.AllowReadingFromString
            };

        public static Dictionary<string, AttributeValue> ToMap<TValue>(TValue value)
            where TValue : class
            => ToMap(value, typeof(TValue));

        public static Dictionary<string, AttributeValue> ToMap(object value, Type type)
        {
            using var jsonDocument = JsonSerializer.SerializeToDocument(value, type, classToMapSerializerOptions);
            if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
            {
                ThrowInvalidOperationExceptionForInvalidRoot(type);
            }
            return ToAttributeMap(jsonDocument.RootElement);
        }

        [DoesNotReturn]
        static void ThrowInvalidOperationExceptionForInvalidRoot(Type type)
            => throw new InvalidOperationException($"Unable to serialize the given type '{type}' because the json kind is not of type 'JsonValueKind.Object'.");

        public static TValue? ToObject<TValue>(Dictionary<string, AttributeValue> attributeValues)
        {
            var jsonObject = ToNode(attributeValues);
            return jsonObject.Deserialize<TValue>(mapToClassSerializerOptions);
        }

        static AttributeValue ToAttribute(JsonElement element) =>
            element.ValueKind switch
            {
                JsonValueKind.Object => ToMapAttribute(element),
                JsonValueKind.Array => ToListAttribute(element),
                JsonValueKind.False => FalseAttributeValue,
                JsonValueKind.True => TrueAttributeValue,
                JsonValueKind.Null => NullAttributeValue,
                JsonValueKind.Number => new AttributeValue { N = element.ToString() },
                JsonValueKind.Undefined => NullAttributeValue,
                JsonValueKind.String => new AttributeValue(element.GetString()),
                _ => ThrowInvalidOperationExceptionForInvalidValueKind(element.ValueKind),
            };

        [DoesNotReturn]
        static AttributeValue ThrowInvalidOperationExceptionForInvalidValueKind(JsonValueKind valueKind)
            => throw new InvalidOperationException($"ValueKind '{valueKind}' could not be mapped.");

        static Dictionary<string, AttributeValue> ToAttributeMap(JsonElement element)
        {
            var dictionary = new Dictionary<string, AttributeValue>();

            foreach (var property in element.EnumerateObject())
            {
                AttributeValue serializeElement = ToAttribute(property.Value);
                if (serializeElement.NULL)
                {
                    continue;
                }
                dictionary.Add(property.Name, serializeElement);
            }

            return dictionary;
        }

        static AttributeValue ToListAttribute(JsonElement element)
        {
            List<AttributeValue>? values = null;
            foreach (var innerElement in element.EnumerateArray())
            {
                values ??= new List<AttributeValue>(element.GetArrayLength());
                AttributeValue serializeElement = ToAttribute(innerElement);
                values.Add(serializeElement);
            }
            return new AttributeValue { L = values ?? new List<AttributeValue>(0) };
        }

        static AttributeValue ToMapAttribute(JsonElement element)
        {
            foreach (var property in element.EnumerateObject())
            {
                if (MemoryStreamConverter.TryExtract(property, out var stream))
                {
                    return new AttributeValue { B = stream };
                }

                if (HashSetMemoryStreamConverter.TryExtract(property, out var streamSet))
                {
                    return new AttributeValue { BS = streamSet };
                }

                if (HashSetOfNumberConverter.TryExtract(property, out var numberSEt))
                {
                    return new AttributeValue { NS = numberSEt };
                }

                if (HashSetStringConverter.TryExtract(property, out var stringSet))
                {
                    return new AttributeValue { SS = stringSet };
                }

                // if we reached this point we know there are no special cases to handle so let's stop trying to iterate
                break;
            }
            return new AttributeValue { M = ToAttributeMap(element) };
        }

        static JsonNode? ToNode(AttributeValue attributeValue) =>
            attributeValue switch
            {
                // check the simple cases first
                { IsBOOLSet: true } => attributeValue.BOOL,
                { NULL: true } => default,
                { N: not null } => JsonNode.Parse(attributeValue.N),
                { S: not null } => attributeValue.S,
                { IsMSet: true, } => ToNode(attributeValue.M),
                { IsLSet: true } => ToNode(attributeValue.L),
                // check the more complex cases last
                { B: not null } => MemoryStreamConverter.ToNode(attributeValue.B),
                { BS.Count: > 0 } => HashSetMemoryStreamConverter.ToNode(attributeValue.BS),
                { SS.Count: > 0 } => JsonSerializer.SerializeToNode(attributeValue.SS),
                { NS.Count: > 0 } => JsonSerializer.SerializeToNode(attributeValue.NS),
                _ => ThrowInvalidOperationExceptionForNonMappableAttribute()
            };

        [DoesNotReturn]
        static JsonNode ThrowInvalidOperationExceptionForNonMappableAttribute()
            => throw new InvalidOperationException("Unable to convert the provided attribute value into a JsonElement");

        static JsonNode ToNode(Dictionary<string, AttributeValue> attributeValues)
        {
            var jsonObject = new JsonObject();
            foreach (var kvp in attributeValues)
            {
                AttributeValue attributeValue = kvp.Value;
                string attributeName = kvp.Key;

                jsonObject.Add(attributeName, ToNode(attributeValue));
            }
            return jsonObject;
        }

        static JsonNode ToNode(List<AttributeValue> attributeValues)
        {
            var array = new JsonArray();
            foreach (var attributeValue in attributeValues)
            {
                array.Add(ToNode(attributeValue));
            }
            return array;
        }

        static readonly AttributeValue NullAttributeValue = new() { NULL = true };
        static readonly AttributeValue TrueAttributeValue = new() { BOOL = true };
        static readonly AttributeValue FalseAttributeValue = new() { BOOL = false };
    }
}