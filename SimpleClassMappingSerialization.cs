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
public class SimpleClassMappingSerialization
{
    private Fixture fixture;
    private Simple simple;

    class Config : ManualConfig
    {
        public Config()
        {
            AddDiagnoser(MemoryDiagnoser.Default);
            AddJob(Job.Default.WithUnrollFactor(5000));
        }
    }

    [GlobalSetup]
    public void GlobalSetup()
    {
        fixture = new Fixture();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        simple = fixture.Create<Simple>();
    }

    [Benchmark(Baseline = true)]
    public Dictionary<string, AttributeValue> Serialize_SDK()
    {
        var jsonString = JsonSerializer.Serialize(simple);
        var doc = Document.FromJson(jsonString);
        return doc.ToAttributeMap();
    }

    [Benchmark()]
    public Dictionary<string, AttributeValue> Serialize_Manual()
    {
        return DataSerializer.Serialize(simple);
    }

    class Simple
    {
        public string String { get; set; }
        public Guid Guid { get; set; }
        public bool Boolean { get; set; }
    }
}