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
public class NestedClassMappingSerialization
{
    private Fixture fixture;
    private Nested nested;

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
        nested = fixture.Create<Nested>();
    }

    [Benchmark(Baseline = true)]
    public Dictionary<string, AttributeValue> Serialize_SDK()
    {
        var jsonString = JsonSerializer.Serialize(nested);
        var doc = Document.FromJson(jsonString);
        return doc.ToAttributeMap();
    }

    [Benchmark()]
    public Dictionary<string, AttributeValue> Serialize_Manual()
    {
        return DataSerializer.Serialize(nested);
    }

    public class Nested
    {
        public string String { get; set; }
        public Guid Guid { get; set; }
        public bool Boolean { get; set; }

        public List<DeeperNested> DeeperNested { get; set; }
    }

    public class DeeperNested
    {
        public string String { get; set; }
        public Guid Guid { get; set; }
        public bool Boolean { get; set; }
    }
}