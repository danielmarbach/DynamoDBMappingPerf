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
public class SimpleClassMappingDeserialization
{
    private Fixture fixture;
    private Dictionary<string,AttributeValue> attributeMap;

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
        var simple = fixture.Create<Simple>();
        var jsonString = JsonSerializer.Serialize(simple);
        var doc = Document.FromJson(jsonString);
        attributeMap = doc.ToAttributeMap();
    }

    [Benchmark(Baseline = true)]
    public Simple Deserialize_SDK()
    {
        var document = Document.FromAttributeMap(attributeMap);
        var jsonString = document.ToJson();
        return JsonSerializer.Deserialize<Simple>(jsonString);
    }

    [Benchmark]
    public Simple Deserialize_Manual()
    {
        return DataSerializer.Deserialize<Simple>(attributeMap);
    }

    public class Simple
    {
        public string String { get; set; }
        public Guid Guid { get; set; }
        public bool Boolean { get; set; }
    }
}