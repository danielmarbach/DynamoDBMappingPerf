#nullable enable
namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using Amazon.DynamoDBv2.Model;

    static class DataSerializer
    {
        static readonly JsonSerializerOptions serializerOptions =
            new JsonSerializerOptions { Converters = { new MemoryStreamConverter() } };

        public static Dictionary<string, AttributeValue> Serialize<TValue>(TValue value)
        {
            using var jsonDocument = JsonSerializer.SerializeToDocument(value, serializerOptions);
            var attributeMapFromDocument = SerializeToAttributeMap(jsonDocument);
            return attributeMapFromDocument;
        }

        public static Dictionary<string, AttributeValue> Serialize(object value, Type type)
        {
            using var jsonDocument = JsonSerializer.SerializeToDocument(value, type, serializerOptions);
            var attributeMapFromDocument = SerializeToAttributeMap(jsonDocument);
            return attributeMapFromDocument;
        }

        public static TValue? Deserialize<TValue>(Dictionary<string, AttributeValue> attributeValues)
        {
            var jsonObject = DeserializeElementFromAttributeMap(attributeValues);
            return jsonObject.Deserialize<TValue>(serializerOptions);
        }

        static Dictionary<string, AttributeValue> SerializeToAttributeMap(JsonDocument document)
        {
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidOperationException("TBD");
            }

            return SerializeElementToAttributeMap(document.RootElement);
        }

        static Dictionary<string, AttributeValue> SerializeElementToAttributeMap(JsonElement element)
        {
            var dictionary = new Dictionary<string, AttributeValue>();

            foreach (var property in element.EnumerateObject())
            {
                AttributeValue serializeElement = SerializeElement(property.Value);
                if (serializeElement.NULL)
                {
                    continue;
                }
                dictionary.Add(property.Name, serializeElement);
            }

            return dictionary;
        }

        static AttributeValue SerializeElement(JsonElement element)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                return SerializeElementToMap(element);
            }

            if (element.ValueKind == JsonValueKind.Array)
            {
                return SerializeElementToList(element);
            }

            if (element.ValueKind == JsonValueKind.False)
            {
                return new AttributeValue { BOOL = false };
            }

            if (element.ValueKind == JsonValueKind.True)
            {
                return new AttributeValue { BOOL = true };
            }

            if (element.ValueKind == JsonValueKind.Null)
            {
                return new AttributeValue { NULL = true };
            }

            if (element.ValueKind == JsonValueKind.Number)
            {
                return new AttributeValue { N = element.ToString() };
            }

            return new AttributeValue(element.GetString());
        }

        static AttributeValue SerializeElementToList(JsonElement element)
        {
            var values = new List<AttributeValue>();
            bool probablyNumberSet = false, probablyStringSet = false, probablyBinarySet = false;
            foreach (var innerElement in element.EnumerateArray())
            {
                AttributeValue serializeElement = SerializeElement(innerElement);
                if (serializeElement.N is not null)
                {
                    probablyNumberSet = true;
                }
                else if (serializeElement.S is not null)
                {
                    probablyStringSet = true;
                }
                else if (serializeElement.B is not null)
                {
                    probablyBinarySet = true;
                }
                values.Add(serializeElement);
            }

            if (probablyNumberSet && !probablyStringSet && !probablyBinarySet)
            {
                var numbersAsString = new List<string>(values.Count);
                for (int index = 0; index < values.Count; index++)
                {
                    AttributeValue? value = values[index];
                    numbersAsString.Add(value.N);
                }
                return new AttributeValue { NS = numbersAsString };
            }
            if (!probablyNumberSet && probablyStringSet && !probablyBinarySet)
            {
                var strings = new List<string>(values.Count);
                for (int index = 0; index < values.Count; index++)
                {
                    AttributeValue? value = values[index];
                    strings.Add(value.S);
                }
                return new AttributeValue { SS = strings };
            }
            if (!probablyNumberSet && !probablyStringSet && probablyBinarySet)
            {
                var memoryStreams = new List<MemoryStream>(values.Count);
                for (int index = 0; index < values.Count; index++)
                {
                    AttributeValue? value = values[index];
                    memoryStreams.Add(value.B);
                }
                return new AttributeValue { BS = memoryStreams };
            }
            return new AttributeValue { L = values };
        }

        static AttributeValue SerializeElementToMap(JsonElement element)
        {
            foreach (var property in element.EnumerateObject())
            {
                if (property.NameEquals(MemoryStreamConverter.PropertyName))
                {
                    return new AttributeValue { B = new MemoryStream(property.Value.GetBytesFromBase64()) };
                }
            }
            return new AttributeValue { M = SerializeElementToAttributeMap(element) };
        }

        static JsonObject DeserializeElementFromAttributeMap(Dictionary<string, AttributeValue> attributeValues)
        {
            var jsonObject = new JsonObject();
            foreach (var kvp in attributeValues)
            {
                AttributeValue attributeValue = kvp.Value;
                string attributeName = kvp.Key;

                jsonObject.Add(attributeName, DeserializeElement(attributeValue));
            }

            return jsonObject;
        }

        static JsonNode? DeserializeElement(AttributeValue attributeValue)
        {
            if (attributeValue.IsMSet)
            {
                return DeserializeElementFromAttributeMap(attributeValue.M);
            }

            if (attributeValue.IsLSet)
            {
                return DeserializeElementFromListSet(attributeValue.L);
            }

            if (attributeValue.B != null)
            {
                return new JsonObject
                {
                    [MemoryStreamConverter.PropertyName] = Convert.ToBase64String(attributeValue.B.ToArray())
                };
            }

            if (attributeValue.BS is { Count: > 0 })
            {
                var array = new JsonArray();
                foreach (var memoryStream in attributeValue.BS)
                {
                    array.Add(new JsonObject
                    {
                        [MemoryStreamConverter.PropertyName] = Convert.ToBase64String(memoryStream.ToArray())
                    });
                }

                return array;
            }

            if (attributeValue.SS is { Count: > 0 })
            {
                var array = new JsonArray();
                foreach (var stringValue in attributeValue.SS)
                {
                    array.Add(stringValue);
                }

                return array;
            }

            if (attributeValue.NS is { Count: > 0 })
            {
                var array = new JsonArray();
                foreach (var numberValue in attributeValue.NS)
                {
                    array.Add(JsonNode.Parse(numberValue));
                }

                return array;
            }

            if (attributeValue.IsBOOLSet)
            {
                return attributeValue.BOOL;
            }

            if (attributeValue.NULL)
            {
                return default;
            }

            if (attributeValue.N != null)
            {
                return JsonNode.Parse(attributeValue.N);
            }

            return attributeValue.S;
        }

        static JsonArray DeserializeElementFromListSet(List<AttributeValue> attributeValues)
        {
            var array = new JsonArray();
            foreach (var attributeValue in attributeValues)
            {
                array.Add(DeserializeElement(attributeValue));
            }
            return array;
        }
    }
}