using System.Text.Json;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using AutoFixture;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using NServiceBus.Persistence.DynamoDB;

namespace DynamoDBMappingPerf;

[Config(typeof(Config))]
public class NestedClassMappingDeserialization
{
    private Fixture? fixture;
    private Dictionary<string,AttributeValue>? attributeMap;

    class Config : ManualConfig
    {
        public Config()
        {
            AddDiagnoser(MemoryDiagnoser.Default);
            AddJob(Job.Default.WithUnrollFactor(1500));
        }
    }

    [GlobalSetup]
    public void GlobalSetup()
    {
        // always five items in list etc.
        fixture = new Fixture { RepeatCount = 5 };
    }

    [IterationSetup]
    public void IterationSetup()
    {
        var nested = fixture.Create<Nested>();
        var jsonString = JsonSerializer.Serialize(nested);
        var doc = Document.FromJson(jsonString);
        attributeMap = doc.ToAttributeMap();
    }

    [Benchmark(Baseline = true)]
    public Nested? Deserialize_SDK()
    {
        var document = Document.FromAttributeMap(attributeMap);
        var jsonString = document.ToJson();
        return JsonSerializer.Deserialize<Nested>(jsonString);
    }

    [Benchmark]
    public Nested? Deserialize_Manual()
    {
        return Mapper.ToObject<Nested>(attributeMap!);
    }

    public class Nested
    {
        public string? String { get; set; }
        public Guid Guid { get; set; }
        public bool Boolean { get; set; }

        public List<DeeperNested>? DeeperNested { get; set; }
    }

    public class DeeperNested
    {
        public string? String { get; set; }
        public Guid Guid { get; set; }
        public bool Boolean { get; set; }
    }
}