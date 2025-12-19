---
title: "Operating Event-Driven Systems in Production"
date: "2025-10-17"
excerpt: "Now comes the hard part: running it reliably in production"
tags: ["EDA", "Kafka", "Production-Ready", "Event-Driven", "Architecture"]
coverImage: "/images/blog/production.jpg"
---

# Part 7: Operating Event-Driven Systems in Production - Complete Guide with Working Demo

## Introduction

You've learned the theory of event-driven architecture, built your first Kafka application, and explored advanced patterns. Now comes the hardest part: **running it reliably in production**. 

In this comprehensive guide, we'll cover everything you need to operate event-driven systems at scale, and then build a complete working demonstration you can run on your laptop.

**What we'll cover:**

**Part A: Production Operations Theory**
* Monitoring and observability
* Debugging distributed systems
* Testing strategies
* Deployment patterns
* Incident response
* Best practices

**Part B: Building a Complete Working Demo**
* Full C# microservices with Kafka
* Prometheus + Grafana + Jaeger observability stack
* Interactive React UI
* Load testing tools
* Step-by-step implementation

Let's dive in!

---

# Part A: Production Operations Theory

## The Three Pillars of Observability
```mermaid

graph LR
    subgraph "Observability"
        M[Metrics<br/>What is happening?]
        L[Logs<br/>Why did it happen?]
        T[Traces<br/>Where did it happen?]
    end
    
    M --> Insight[System Insight]
    L --> Insight
    T --> Insight
    
    style Insight fill:#00b894
````

### 1. Metrics - Key Performance Indicators

**Producer Metrics to Track:**
````csharp
using Prometheus;

// Events produced
var eventsProduced = Metrics.CreateCounter(
    "events_produced_total",
    "Total events produced",
    new CounterConfiguration { LabelNames = new[] { "topic", "event_type" } });

// Production failures
var eventsFailed = Metrics.CreateCounter(
    "events_failed_total",
    "Total failed event productions",
    new CounterConfiguration { LabelNames = new[] { "topic", "error_type" } });

// Production latency
var productionDuration = Metrics.CreateHistogram(
    "event_production_duration_seconds",
    "Time to produce event",
    new HistogramConfiguration { LabelNames = new[] { "topic" } });

// Usage
eventsProduced.WithLabels("orders", "OrderPlaced").Inc();
using (productionDuration.WithLabels("orders").NewTimer())
{
    await producer.ProduceAsync("orders", message);
}
````

**Consumer Metrics to Track:**
````csharp
// Consumer lag - CRITICAL metric
var consumerLag = Metrics.CreateGauge(
    "consumer_lag",
    "Consumer lag in messages",
    new GaugeConfiguration { 
        LabelNames = new[] { "consumer_group", "topic", "partition" } 
    });

// Events consumed
var eventsConsumed = Metrics.CreateCounter(
    "events_consumed_total",
    "Total events consumed",
    new CounterConfiguration { 
        LabelNames = new[] { "consumer_group", "topic", "event_type" } 
    });

// Processing duration
var processingDuration = Metrics.CreateHistogram(
    "event_processing_duration_seconds",
    "Time to process event",
    new HistogramConfiguration { 
        LabelNames = new[] { "consumer_group", "event_type" } 
    });

// Processing errors
var processingErrors = Metrics.CreateCounter(
    "processing_errors_total",
    "Total processing errors",
    new CounterConfiguration { 
        LabelNames = new[] { "consumer_group", "error_type" } 
    });
````

**Business Metrics:**
````csharp
// Track business outcomes, not just technical metrics
var ordersPlaced = Metrics.CreateCounter(
    "orders_placed_total",
    "Total orders placed",
    new CounterConfiguration { LabelNames = new[] { "status" } });

var orderValue = Metrics.CreateHistogram(
    "order_value_dollars",
    "Order value in dollars",
    new HistogramConfiguration { 
        Buckets = new[] { 10, 50, 100, 500, 1000, 5000 } 
    });

var sagaDuration = Metrics.CreateHistogram(
    "saga_duration_seconds",
    "Saga execution time",
    new HistogramConfiguration { 
        LabelNames = new[] { "saga_type", "status" } 
    });
````

### 2. Distributed Tracing - Following Requests Across Services

**OpenTelemetry Setup in C#:**
````csharp
using OpenTelemetry;
using OpenTelemetry.Trace;
using OpenTelemetry.Resources;
using System.Diagnostics;

// In Program.cs
builder.Services.AddOpenTelemetry()
    .WithTracing(tracerProvider =>
    {
        tracerProvider
            .SetResourceBuilder(ResourceBuilder.CreateDefault()
                .AddService("order-service"))
            .AddSource("order-service")
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddJaegerExporter(options =>
            {
                options.AgentHost = "localhost";
                options.AgentPort = 6831;
            });
    });

// Create ActivitySource
var activitySource = new ActivitySource("order-service");

// Producer with tracing
public async Task ProduceOrderEvent(Order order)
{
    using var activity = activitySource.StartActivity(
        "produce_order_event",
        ActivityKind.Producer);
    
    activity?.SetTag("order.id", order.OrderId);
    activity?.SetTag("order.amount", order.TotalAmount);
    
    // Add correlation ID to event
    var correlationId = activity?.Id ?? Guid.NewGuid().ToString();
    
    var eventData = new OrderPlacedEvent
    {
        EventId = Guid.NewGuid().ToString(),
        CorrelationId = correlationId,
        OrderId = order.OrderId,
        // ... rest of event
    };
    
    // Add trace context to Kafka headers
    var headers = new Headers();
    headers.Add("traceparent", Encoding.UTF8.GetBytes(activity?.Id ?? ""));
    
    await producer.ProduceAsync("orders", new Message<string, string>
    {
        Key = order.OrderId,
        Value = JsonSerializer.Serialize(eventData),
        Headers = headers
    });
    
    activity?.AddEvent(new ActivityEvent("Event published to Kafka"));
}

// Consumer with tracing
public async Task ConsumeEvents()
{
    foreach (var message in consumer.Consume())
    {
        var eventData = JsonSerializer.Deserialize<OrderPlacedEvent>(message.Value);
        
        // Extract trace context
        var traceParent = message.Headers
            .FirstOrDefault(h => h.Key == "traceparent")
            ?.GetValueBytes();
        
        var parentContext = traceParent != null 
            ? ActivityContext.Parse(Encoding.UTF8.GetString(traceParent), null)
            : default;
        
        using var activity = activitySource.StartActivity(
            "process_order_event",
            ActivityKind.Consumer,
            parentContext);
        
        activity?.SetTag("event.type", eventData.EventType);
        activity?.SetTag("order.id", eventData.OrderId);
        
        try
        {
            await ProcessOrder(eventData);
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.RecordException(ex);
            throw;
        }
    }
}
````

### 3. Correlation IDs - Tracking Requests End-to-End
````csharp
// ASP.NET Core Middleware for Correlation IDs
public class CorrelationMiddleware
{
    private readonly RequestDelegate _next;
    private const string CorrelationIdHeader = "X-Correlation-ID";

    public CorrelationMiddleware(RequestDelegate next)
    {
        _next = next;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        var correlationId = context.Request.Headers[CorrelationIdHeader]
            .FirstOrDefault() ?? Guid.NewGuid().ToString();

        context.Items["CorrelationId"] = correlationId;
        context.Response.Headers[CorrelationIdHeader] = correlationId;

        using (Activity.Current?.AddBaggage("correlation_id", correlationId))
        {
            await _next(context);
        }
    }
}

// Structured logging with correlation ID
public class CorrelationLogEnricher : ILogEventEnricher
{
    private readonly IHttpContextAccessor _httpContextAccessor;

    public CorrelationLogEnricher(IHttpContextAccessor httpContextAccessor)
    {
        _httpContextAccessor = httpContextAccessor;
    }

    public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
    {
        var correlationId = _httpContextAccessor.HttpContext?
            .Items["CorrelationId"]?.ToString();
        
        if (correlationId != null)
        {
            logEvent.AddPropertyIfAbsent(
                propertyFactory.CreateProperty("CorrelationId", correlationId));
        }
    }
}
````

## Debugging Distributed Systems

### Common Issue #1: Event Lost
```mermaid

graph TD
    A[Event Lost?] --> B{Check Producer Logs}
    B -->|No send| C[Producer never sent]
    B -->|Sent| D{Check Kafka Topic}
    
    D -->|Not in topic| E[Check ACK settings<br/>Check broker logs]
    D -->|In topic| F{Check Consumer}
    
    F -->|Not consuming| G[Check consumer<br/>subscription/offset]
    F -->|Consuming| H{Check Processing}
    
    H -->|Not processing| I[Check consumer logs<br/>for errors]
    
    style C fill:#ff7675
    style E fill:#fdcb6e
    style I fill:#fdcb6e
````

**Debug Script in C#:**
````csharp
public async Task DebugLostEvent(string eventId)
{
    Console.WriteLine($"🔍 Debugging event: {eventId}");
    
    // 1. Check producer logs
    var producerLogs = await SearchLogs("order-service", eventId);
    if (producerLogs.Count == 0)
    {
        Console.WriteLine("❌ Event never produced");
        return;
    }
    Console.WriteLine("✅ Event was produced");
    
    // 2. Check Kafka topic
    var foundInKafka = await SearchKafkaTopic("orders", eventId);
    if (!foundInKafka.Found)
    {
        Console.WriteLine("❌ Event not in Kafka - check producer ACK settings");
        var brokerLogs = await GetBrokerLogs(producerLogs.Timestamp);
        Console.WriteLine($"   Broker logs: {brokerLogs}");
        return;
    }
    Console.WriteLine($"✅ Event in Kafka at offset {foundInKafka.Offset}");
    
    // 3. Check consumer offset
    var consumerOffset = await GetConsumerOffset(
        "email-service", "orders", foundInKafka.Partition);
    
    if (consumerOffset < foundInKafka.Offset)
    {
        var lag = foundInKafka.Offset - consumerOffset;
        Console.WriteLine($"⚠️  Consumer hasn't reached this offset yet (lag: {lag})");
        return;
    }
    Console.WriteLine("✅ Consumer should have processed this event");
    
    // 4. Check consumer logs
    var consumerLogs = await SearchLogs("email-service", eventId);
    if (consumerLogs.Count == 0)
    {
        Console.WriteLine("❌ Consumer didn't process event - check for errors");
        var errors = await GetConsumerErrors("email-service");
        Console.WriteLine($"   Recent errors: {string.Join(", ", errors)}");
        return;
    }
    Console.WriteLine("✅ Consumer processed event");
}
````

### Common Issue #2: High Consumer Lag
````csharp
public async Task DiagnoseConsumerLag(string consumerGroup, string topic)
{
    Console.WriteLine($"📊 Consumer Lag Analysis for {consumerGroup}");
    
    var lagInfo = await GetConsumerLag(consumerGroup, topic);
    
    Console.WriteLine($"Total Lag: {lagInfo.TotalLag} messages");
    Console.WriteLine($"Partitions: {lagInfo.PartitionCount}");
    Console.WriteLine($"Consumers: {lagInfo.ConsumerCount}");
    
    // Check 1: Not enough consumers?
    if (lagInfo.ConsumerCount < lagInfo.PartitionCount)
    {
        Console.WriteLine($"⚠️  Only {lagInfo.ConsumerCount} consumers " +
            $"for {lagInfo.PartitionCount} partitions");
        Console.WriteLine("   Recommendation: Add more consumers");
    }
    
    // Check 2: Slow processing?
    var avgProcessingTime = await GetAvgProcessingTime(consumerGroup);
    if (avgProcessingTime > TimeSpan.FromSeconds(1))
    {
        Console.WriteLine($"⚠️  Slow processing: {avgProcessingTime.TotalSeconds:F2}s average");
        Console.WriteLine("   Recommendations:");
        Console.WriteLine("   - Optimize processing logic");
        Console.WriteLine("   - Add parallelism");
        Console.WriteLine("   - Check external dependencies");
    }
    
    // Check 3: Frequent rebalancing?
    var rebalanceCount = await GetRebalanceCount(consumerGroup, TimeSpan.FromHours(1));
    if (rebalanceCount > 5)
    {
        Console.WriteLine($"⚠️  Frequent rebalancing: {rebalanceCount} times in last hour");
        Console.WriteLine("   Check:");
        Console.WriteLine("   - session.timeout.ms");
        Console.WriteLine("   - max.poll.interval.ms");
        Console.WriteLine("   - Consumer stability");
    }
    
    // Check 4: Production rate spike?
    var productionRate = await GetProductionRate(topic, TimeSpan.FromMinutes(5));
    var consumptionRate = await GetConsumptionRate(
        consumerGroup, topic, TimeSpan.FromMinutes(5));
    
    if (productionRate > consumptionRate * 1.5)
    {
        Console.WriteLine($"⚠️  Production outpacing consumption");
        Console.WriteLine($"   Production: {productionRate:F0} msgs/sec");
        Console.WriteLine($"   Consumption: {consumptionRate:F0} msgs/sec");
        Console.WriteLine("   Recommendation: Scale consumers");
    }
}
````

## Alert Rules with Prometheus

**alerts.yml:**
````yaml
groups:
  - name: consumer_lag
    interval: 30s
    rules:
      - alert: HighConsumerLag
        expr: consumer_lag > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High consumer lag detected"
          description: "Consumer group {{ $labels.consumer_group }} has lag of {{ $value }} messages"
      
      - alert: CriticalConsumerLag
        expr: consumer_lag > 50000
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Critical consumer lag"
          description: "Consumer group {{ $labels.consumer_group }} is severely behind"
  
  - name: processing_errors
    interval: 30s
    rules:
      - alert: HighErrorRate
        expr: rate(processing_errors_total[5m]) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate in event processing"
          description: "Error rate is {{ $value }} errors/sec for {{ $labels.consumer_group }}"
  
  - name: kafka_health
    interval: 30s
    rules:
      - alert: UnderReplicatedPartitions
        expr: kafka_server_replicamanager_underreplicatedpartitions > 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Under-replicated partitions detected"
          description: "{{ $value }} partitions are under-replicated"
````

## Testing Strategies

### Unit Testing
````csharp
[Fact]
public async Task OrderCommandHandler_PlacesOrder_PublishesEvent()
{
    // Arrange
    var mockRepository = new Mock<IOrderRepository>();
    var mockEventBus = new Mock<IEventBus>();
    var handler = new OrderCommandHandler(mockRepository.Object, mockEventBus.Object);
    
    var command = new PlaceOrderCommand
    {
        CustomerId = "CUST-123",
        Items = new List<OrderItem>
        {
            new() { ProductId = "PROD-001", Quantity = 2, Price = 29.99m }
        }
    };
    
    // Act
    var orderId = await handler.HandlePlaceOrder(command);
    
    // Assert
    Assert.NotNull(orderId);
    mockRepository.Verify(r => r.SaveAsync(It.IsAny<Order>()), Times.Once);
    mockEventBus.Verify(e => e.PublishAsync(
        It.Is<OrderPlacedEvent>(evt => evt.OrderId == orderId)), 
        Times.Once);
}
````

### Integration Testing with Testcontainers
````csharp
public class KafkaIntegrationTests : IAsyncLifetime
{
    private KafkaContainer _kafka;
    private IProducer<string, string> _producer;
    private IConsumer<string, string> _consumer;

    public async Task InitializeAsync()
    {
        _kafka = new KafkaBuilder().Build();
        await _kafka.StartAsync();
        
        var config = new ProducerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress()
        };
        _producer = new ProducerBuilder<string, string>(config).Build();
        
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            GroupId = "test-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
    }

    [Fact]
    public async Task EndToEnd_ProduceAndConsume_Success()
    {
        // Arrange
        const string topic = "test-topic";
        _consumer.Subscribe(topic);
        
        var testEvent = new { OrderId = "ORD-123", Amount = 99.99 };
        var message = new Message<string, string>
        {
            Key = testEvent.OrderId,
            Value = JsonSerializer.Serialize(testEvent)
        };
        
        // Act
        await _producer.ProduceAsync(topic, message);
        _producer.Flush(TimeSpan.FromSeconds(10));
        
        var result = _consumer.Consume(TimeSpan.FromSeconds(10));
        
        // Assert
        Assert.NotNull(result);
        var receivedEvent = JsonSerializer.Deserialize<dynamic>(result.Message.Value);
        Assert.Equal("ORD-123", receivedEvent.OrderId.ToString());
    }

    public async Task DisposeAsync()
    {
        _producer?.Dispose();
        _consumer?.Dispose();
        await _kafka.DisposeAsync();
    }
}
````

### Load Testing
````csharp
public class LoadTestResult
{
    public TimeSpan TotalDuration { get; set; }
    public int TotalRequests { get; set; }
    public int SuccessfulRequests { get; set; }
    public int FailedRequests { get; set; }
    public double AverageDuration { get; set; }
    public double P95Duration { get; set; }
    public double P99Duration { get; set; }
}

public async Task<LoadTestResult> RunLoadTest(
    int requestCount, 
    bool concurrent)
{
    var stopwatch = Stopwatch.StartNew();
    var durations = new List<double>();
    var successful = 0;
    var failed = 0;

    if (concurrent)
    {
        var tasks = Enumerable.Range(0, requestCount)
            .Select(async i =>
            {
                var sw = Stopwatch.StartNew();
                try
                {
                    await TriggerEvent($"EVENT-{i}");
                    Interlocked.Increment(ref successful);
                }
                catch
                {
                    Interlocked.Increment(ref failed);
                }
                sw.Stop();
                lock (durations)
                {
                    durations.Add(sw.Elapsed.TotalMilliseconds);
                }
            });
        
        await Task.WhenAll(tasks);
    }
    else
    {
        for (int i = 0; i < requestCount; i++)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                await TriggerEvent($"EVENT-{i}");
                successful++;
            }
            catch
            {
                failed++;
            }
            sw.Stop();
            durations.Add(sw.Elapsed.TotalMilliseconds);
        }
    }

    stopwatch.Stop();
    durations.Sort();

    return new LoadTestResult
    {
        TotalDuration = stopwatch.Elapsed,
        TotalRequests = requestCount,
        SuccessfulRequests = successful,
        FailedRequests = failed,
        AverageDuration = durations.Average(),
        P95Duration = durations[(int)(durations.Count * 0.95)],
        P99Duration = durations[(int)(durations.Count * 0.99)]
    };
}
````

## Deployment Strategies

### Blue-Green Deployment
````yaml
# Deploy new version (green) alongside old version (blue)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-processor-v2
spec:
  replicas: 3
  selector:
    matchLabels:
      app: order-processor
      version: v2
  template:
    metadata:
      labels:
        app: order-processor
        version: v2
    spec:
      containers:
      - name: order-processor
        image: order-processor:v2
        env:
        - name: KAFKA_GROUP_ID
          value: "order-processor-green"
````

**Process:**
1. Deploy new version with different consumer group
2. Both versions consume same events
3. Monitor green deployment
4. If healthy, scale down blue
5. If issues, rollback to blue instantly

### Canary Deployment
````csharp
// Route percentage of traffic to new version
public class CanaryRouter
{
    private readonly int _canaryPercentage;

    public CanaryRouter(int canaryPercentage)
    {
        _canaryPercentage = canaryPercentage;
    }

    public string GetConsumerGroup(string messageKey)
    {
        var hash = Math.Abs(messageKey.GetHashCode()) % 100;
        return hash < _canaryPercentage 
            ? "processor-canary" 
            : "processor-stable";
    }
}

// Gradually increase: 5% → 10% → 25% → 50% → 100%
````

## Incident Response Runbooks

### Runbook: High Consumer Lag
````markdown
# Runbook: High Consumer Lag Alert

## Symptoms
- Alert: HighConsumerLag
- Consumer group falling behind
- Increasing lag metric

## Impact
- Events processed with delay
- Real-time features degraded
- Potential data loss if retention expires

## Investigation Steps

1. Check current lag:
```bash
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group order-processor --describe
```

2. Check consumer health:
```bash
kubectl get pods -l app=order-processor
kubectl logs -l app=order-processor --tail=100
```

3. Check processing time in Grafana

4. Check rebalancing frequency

## Resolution

### If: Not enough consumers
```bash
kubectl scale deployment/order-processor --replicas=6
```

### If: Slow processing
- Check external dependencies
- Look for slow queries
- Consider adding caching

### If: Frequent rebalancing
- Increase session timeout
- Increase max poll interval
- Fix consumer stability issues

### If: Production spike
- Temporary: Pause non-critical consumers
- Long-term: Implement auto-scaling

## Prevention
- Set up auto-scaling based on lag
- Monitor processing time trends
- Regular load testing
````

## Best Practices Checklist

### Development
- ✅ Use correlation IDs for tracing
- ✅ Implement idempotent consumers
- ✅ Use outbox pattern for atomic writes
- ✅ Version all events
- ✅ Make events self-contained
- ✅ Use meaningful event names (past tense)

### Operations
- ✅ Monitor consumer lag
- ✅ Alert on high error rates
- ✅ Track end-to-end latency
- ✅ Set up distributed tracing
- ✅ Implement health checks
- ✅ Use circuit breakers for external calls

### Reliability
- ✅ Enable producer idempotence
- ✅ Use appropriate ACK level (acks=all)
- ✅ Set proper replication factor (3+)
- ✅ Configure retry policies
- ✅ Implement dead letter queues
- ✅ Regular backup and disaster recovery tests

### Performance
- ✅ Tune batch sizes
- ✅ Enable compression
- ✅ Use appropriate partition count
- ✅ Monitor and optimize processing time
- ✅ Scale consumers based on lag
- ✅ Use connection pooling

### Security
- ✅ Enable SSL/TLS
- ✅ Implement authentication (SASL)
- ✅ Set up ACLs
- ✅ Encrypt sensitive data
- ✅ Regular security audits
- ✅ Rotate credentials

---

# Part B: Building a Complete Working Demo

Now that we understand the theory, let's build a **production-ready demonstration** that implements everything we've learned. This demo runs entirely on your laptop and includes:

- **3 C# Microservices** communicating via Kafka
- **Full Observability Stack** (Prometheus, Grafana, Jaeger)
- **Interactive React UI** to trigger events
- **Load Testing** to simulate production scenarios
- **Complete Docker Compose** setup

## What We'll Build: Event-Driven NAV Calculator

A realistic financial services scenario: calculating Net Asset Value (NAV) for investment funds.
````
┌─────────────┐      pricing-updates       ┌──────────────┐
│   Pricing   ├───────────────────────────>│     NAV      │
│   Service   │        (Kafka)             │  Calculator  │
└─────────────┘                            └──────┬───────┘
                                                  │
                                                  │ nav-calculated
                                                  ↓
                                           ┌──────────────┐
                                           │ Notification │
                                           │   Service    │
                                           └──────────────┘
````

**Flow:**
1. User triggers pricing for a fund
2. Pricing Service publishes `PricingUpdate` event
3. NAV Calculator (2 instances) consume and calculate NAV
4. NAV Calculator publishes `NAVCalculated` event
5. Notification Service sends notifications

**All observable via:**
- Prometheus metrics
- Grafana dashboards
- Jaeger distributed traces
- Structured logs with correlation IDs

## Project Structure
````
event-driven-nav-poc/
├── src/
│   ├── Shared/
│   │   ├── Shared.Observability/       # Reusable metrics, tracing
│   │   └── Shared.Kafka/               # Kafka wrappers with observability
│   ├── Services/
│   │   ├── PricingService/             # Triggers pricing updates
│   │   ├── NavCalculator/              # Calculates NAV (2 instances)
│   │   └── NotificationService/        # Sends notifications
│   └── Tools/
│       └── LoadTester/                 # Performance testing tool
├── demo-ui/                            # React UI
├── monitoring/                         # Prometheus + Grafana config
├── docker-compose.yml                  # Complete stack
└── README.md
````

## Implementation Highlights

### 1. Reusable Observability Library
````csharp
// Shared.Observability/ServiceMetrics.cs
public class ServiceMetrics
{
    private readonly string _serviceName;
    private readonly Counter _eventsProcessed;
    private readonly Histogram _processingDuration;

    public ServiceMetrics(string serviceName)
    {
        _serviceName = serviceName;
        
        _eventsProcessed = Metrics.CreateCounter(
            "events_processed_total",
            "Total events processed",
            new CounterConfiguration
            {
                LabelNames = new[] { "service", "event_type", "status" }
            });

        _processingDuration = Metrics.CreateHistogram(
            "event_processing_duration_seconds",
            "Time to process event",
            new HistogramConfiguration
            {
                LabelNames = new[] { "service", "event_type" },
                Buckets = Histogram.ExponentialBuckets(0.001, 2, 10)
            });
    }

    public IDisposable TrackEventProcessing(string eventType)
    {
        return new EventProcessingTracker(this, eventType);
    }

    public void RecordEventProcessed(string eventType, string status)
    {
        _eventsProcessed
            .WithLabels(_serviceName, eventType, status)
            .Inc();
    }

    private class EventProcessingTracker : IDisposable
    {
        private readonly ServiceMetrics _metrics;
        private readonly string _eventType;
        private readonly Stopwatch _stopwatch;

        public EventProcessingTracker(ServiceMetrics metrics, string eventType)
        {
            _metrics = metrics;
            _eventType = eventType;
            _stopwatch = Stopwatch.StartNew();
        }

        public void Dispose()
        {
            _stopwatch.Stop();
            _metrics._processingDuration
                .WithLabels(_metrics._serviceName, _eventType)
                .Observe(_stopwatch.Elapsed.TotalSeconds);
        }
    }
}
````

### 2. Observable Kafka Wrapper
````csharp
// Shared.Kafka/ObservableKafkaProducer.cs
public class ObservableKafkaProducer<TKey, TValue> : IDisposable
{
    private readonly IProducer<TKey, TValue> _producer;
    private readonly ServiceMetrics _metrics;
    private readonly ActivitySource _activitySource;

    public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic,
        Message<TKey, TValue> message,
        string? correlationId = null)
    {
        using var activity = _activitySource.StartActivity(
            "produce_event",
            ActivityKind.Producer);

        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination", topic);
        activity?.SetTag("correlation_id", correlationId ?? Guid.NewGuid().ToString());

        try
        {
            // Add correlation ID and trace context to headers
            message.Headers ??= new Headers();
            
            if (correlationId != null)
            {
                message.Headers.Add("correlation_id", 
                    Encoding.UTF8.GetBytes(correlationId));
            }

            if (activity != null)
            {
                message.Headers.Add("traceparent",
                    Encoding.UTF8.GetBytes(activity.Id ?? ""));
            }

            var result = await _producer.ProduceAsync(topic, message);
            
            activity?.SetTag("messaging.kafka.partition", result.Partition.Value);
            activity?.SetTag("messaging.kafka.offset", result.Offset.Value);
            
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    public void Dispose() => _producer?.Dispose();
}
````

### 3. NAV Calculator Service (Consumer)
````csharp
// Services/NavCalculator/NavCalculatorConsumer.cs
public class NavCalculatorConsumer : BackgroundService
{
    private readonly ILogger<NavCalculatorConsumer> _logger;
    private readonly ServiceMetrics _metrics;
    private readonly string _instanceId;
    private ObservableKafkaConsumer<string, string> _consumer;
    private ObservableKafkaProducer<string, string> _producer;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe("pricing-updates");

        _logger.LogInformation(
            "🎧 [{InstanceId}] Starting to consume pricing updates...", 
            _instanceId);

        while (!stoppingToken.IsCancellationRequested)
        {
            await _consumer.ConsumeAsync(async (result) =>
            {
                var pricingEvent = JsonSerializer
                    .Deserialize<PricingUpdateEvent>(result.Message.Value);

                if (pricingEvent != null)
                {
                    await ProcessPricingUpdate(pricingEvent);
                }
            }, stoppingToken);
        }
    }

    private async Task ProcessPricingUpdate(PricingUpdateEvent pricingEvent)
    {
        using var activity = TracingSetup.ActivitySource
            .StartActivity("calculate_nav");
        
        activity?.SetTag("fund.id", pricingEvent.FundId);
        activity?.SetTag("correlation_id", pricingEvent.CorrelationId);
        activity?.SetTag("instance_id", _instanceId);

        // Simulate calculation (100-500ms)
        var calculationTime = Random.Shared.Next(100, 500);
        await Task.Delay(calculationTime);

        var totalValue = pricingEvent.Prices.Sum(p => p.Price * p.Quantity);
        var sharesOutstanding = Random.Shared.Next(100000, 1000000);
        var navPerShare = totalValue / sharesOutstanding;

        var navEvent = new NavCalculatedEvent
        {
            CorrelationId = pricingEvent.CorrelationId,
            FundId = pricingEvent.FundId,
            NavPerShare = Math.Round(navPerShare, 4),
            TotalAssets = totalValue,
            SharesOutstanding = sharesOutstanding,
            CalculatedAt = DateTime.UtcNow,
            CalculatedBy = _instanceId
        };

        await _producer.ProduceAsync(
            "nav-calculated",
            new Message<string, string>
            {
                Key = pricingEvent.FundId,
                Value = JsonSerializer.Serialize(navEvent)
            },
            pricingEvent.CorrelationId);

        _logger.LogInformation(
            "✅ [{InstanceId}] Calculated NAV for {FundId}: ${Nav:F4}",
            _instanceId, pricingEvent.FundId, navPerShare);
    }
}
````

### 4. Interactive React UI

The UI provides:
- **Fund selection** buttons to trigger individual calculations
- **Month-End simulation** button to process all funds simultaneously
- **Real-time results** display with correlation IDs
- **Statistics panel** showing success rate, avg duration
- **Links to observability tools** (Grafana, Jaeger, Prometheus)

Key features:
````jsx
const triggerPricing = async (fundId, fundName) => {
  const startTime = Date.now();
  
  const response = await axios.post(
    `${API_BASE_URL}/api/pricing/trigger`,
    { fundId, fundName }
  );
  
  const duration = Date.now() - startTime;
  
  setResults(prev => [
    { ...response.data, duration, status: 'success' },
    ...prev
  ].slice(0, 20));
};

const triggerMonthEnd = async () => {
  // Trigger all funds in parallel (simulates month-end spike)
  const promises = funds.map(fund => 
    triggerPricing(fund.id, fund.name)
  );
  
  await Promise.all(promises);
};
````

### 5. Load Testing Tool
````csharp
// Tools/LoadTester/Program.cs
var rootCommand = new RootCommand("Event-Driven NAV Calculator Load Tester");

var fundCountOption = new Option<int>("--funds", () => 28);
var concurrentOption = new Option<bool>("--concurrent", () => true);
var iterationsOption = new Option<int>("--iterations", () => 1);

rootCommand.SetHandler(async (fundCount, concurrent, iterations) =>
{
    await RunLoadTest(fundCount, concurrent, iterations);
}, fundCountOption, concurrentOption, iterationsOption);

// Pretty output with Spectre.Console
AnsiConsole.Write(
    new FigletText("Load Tester")
        .LeftJustified()
        .Color(Color.Blue));

// Progress bar during test
await AnsiConsole.Progress()
    .Columns(new ProgressColumn[]
    {
        new TaskDescriptionColumn(),
        new ProgressBarColumn(),
        new PercentageColumn(),
        new RemainingTimeColumn(),
    })
    .StartAsync(async ctx => { /* ... */ });

// Results table
var table = new Table();
table.AddColumn("[bold]Metric[/]");
table.AddColumn("[bold]Value[/]");
table.AddRow("Total Duration", $"[green]{totalDuration:F2}s[/]");
table.AddRow("Success Rate", $"{successRate:F2}%");
table.AddRow("P95 Duration", $"{p95:F0}ms");
AnsiConsole.Write(table);
````

### 6. Grafana Dashboard

Pre-configured dashboard showing:
- **Events processed per second** (by service)
- **Processing duration** (P50, P95, P99)
- **Success rate gauge**
- **Error count**
- **Average processing time**
- **Load distribution** across NAV calculator instances

### 7. Docker Compose - Complete Stack
````yaml
version: '3.8'

services:
  kafka:
    image: apache/kafka:3.7.0
    # KRaft mode - no ZooKeeper needed
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      # ... configuration
  
  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
  
  grafana:
    image: grafana/grafana:latest
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - ./monitoring/grafana-dashboards:/etc/grafana/provisioning/dashboards
  
  jaeger:
    image: jaegertracing/all-in-one:latest
  
  pricing-service:
    build:
      context: .
      dockerfile: src/Services/PricingService/Dockerfile
  
  nav-calculator-1:
    build:
      context: .
      dockerfile: src/Services/NavCalculator/Dockerfile
    environment:
      - INSTANCE_ID=nav-calculator-1
  
  nav-calculator-2:
    build:
      context: .
      dockerfile: src/Services/NavCalculator/Dockerfile
    environment:
      - INSTANCE_ID=nav-calculator-2
  
  notification-service:
    build:
      context: .
      dockerfile: src/Services/NotificationService/Dockerfile
  
  demo-ui:
    build:
      context: ./demo-ui
````

## Running the Demo

### Quick Start
````bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/event-driven-nav-poc
cd event-driven-nav-poc

# Start everything
docker-compose up --build

# Wait ~2 minutes, then open:
# - http://localhost:3001 (Demo UI)
# - http://localhost:3000 (Grafana - admin/admin)
# - http://localhost:16686 (Jaeger)
````

### What to Try

1. **Single Fund Calculation**
   - Click any fund button in the UI
   - Watch logs in terminal
   - Check Grafana for metrics spike
   - Search Jaeger by correlation ID

2. **Month-End Spike**
   - Click "Simulate Month-End"
   - Watch 5 funds process simultaneously
   - Observe load distribution across 2 NAV calculator instances
   - Check P95 latency in Grafana

3. **Load Testing**
````bash
   cd src/Tools/LoadTester
   dotnet run -- --funds 28 --concurrent true --iterations 3
````
   
   Output:
````
   ┌─────────────────┬──────────────┐
   │ Metric          │ Value        │
   ├─────────────────┼──────────────┤
   │ Total Duration  │ 2.34s        │
   │ Successful      │ 28           │
   │ P95 Duration    │ 487ms        │
   │ Throughput      │ 11.97 f/s    │
   └─────────────────┴──────────────┘
````

4. **Distributed Tracing**
   - Trigger an event from UI
   - Copy correlation ID from result
   - Paste into Jaeger search
   - See complete trace across all 3 services

5. **Simulate Failure**
````bash
   # Stop one NAV calculator instance
   docker stop nav-calculator-2
   
   # Trigger events - watch other instance handle all load
   # Check Grafana for increased load on nav-calculator-1
   
   # Restart
   docker start nav-calculator-2
````

## Key Learnings from This Demo

### 1. Observability is Essential

Without Prometheus + Grafana + Jaeger, you're flying blind:
- Metrics show **what** is happening
- Logs explain **why** it happened
- Traces reveal **where** in the flow it happened

### 2. Correlation IDs Enable Debugging

Following a single request through multiple services is impossible without correlation IDs. With them, you can:
- Search logs across all services
- Find the exact trace in Jaeger
- Debug issues that span multiple hops

### 3. Horizontal Scaling Just Works

When we run 2 NAV calculator instances:
- Kafka automatically distributes partitions
- Load balances across instances
- Rebalances if one dies

### 4. Load Testing Reveals Bottlenecks

Running 28 concurrent requests shows:
- Where your slowest dependencies are
- If your consumers can keep up
- What happens under month-end load

### 5. Production Patterns Matter

This demo implements real production patterns:
- Idempotent producers
- At-least-once delivery
- Structured logging
- Health checks
- Graceful shutdown

## Adapting This for Your Domain

This demo uses NAV calculation, but the patterns apply to any domain:

**E-commerce:**
- Order Service → Inventory Service → Shipping Service

**IoT:**
- Device Telemetry → Processing Service → Alert Service

**Financial Services:**
- Trade Execution → Risk Check → Settlement

**The architecture is identical:**
1. Event producer publishes to Kafka
2. Multiple consumers process in parallel
3. Each consumer publishes downstream events
4. Everything is observable via Prometheus/Grafana/Jaeger

## Best Practices Demonstrated

✅ **Separation of Concerns** - Each service has one job  
✅ **Observability First** - Metrics, logs, traces from day 1  
✅ **Horizontal Scalability** - Easy to add more instances  
✅ **Resilience** - Services can fail independently  
✅ **Testability** - Load testing built in  
✅ **Developer Experience** - One command to start everything  
✅ **Production Ready** - Implements real production patterns  

## Next Steps

### For Learning:
- Modify the demo to add another service
- Change the event schema and see schema evolution
- Implement a dead letter queue
- Add a saga pattern

### For Production:
- Add Kubernetes manifests
- Implement auto-scaling based on lag
- Set up CI/CD pipeline
- Add authentication/authorization
- Implement data encryption

## Conclusion

Operating event-driven systems in production requires:

**Theory (Part A):**
- Understanding the three pillars of observability
- Knowing how to debug distributed systems
- Having comprehensive testing strategies
- Planning deployment approaches
- Preparing incident response procedures

**Practice (Part B):**
- Actually building the observability stack
- Instrumenting your code properly
- Testing under realistic load
- Experiencing failures and recovery
- Iterating based on real metrics

This guide gives you both. The theory ensures you understand **why** things work a certain way. The working demo lets you **see it in action** and **experiment safely**.

**You're now ready to build and operate production event-driven systems!** 🚀

Remember:
- Start simple, add complexity as needed
- Monitor from day one
- Test failure scenarios
- Document everything
- Learn from incidents

The full source code is available at: [GitHub Repository]

Good luck with your event-driven journey!

---

## Additional Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Prometheus Best Practices](https://prometheus.io/docs/practices/)
- [OpenTelemetry .NET](https://opentelemetry.io/docs/instrumentation/net/)
- [Confluent Kafka .NET Client](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html)

---

**Built with ❤️ for the Event-Driven Architecture blog series**
