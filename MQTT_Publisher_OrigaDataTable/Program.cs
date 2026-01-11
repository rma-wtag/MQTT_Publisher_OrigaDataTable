using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using MQTT_Publisher_OrigaDataTable;
using MQTTnet;
using MQTTnet.Client;

class Program
{

    private static IMqttClient? client;
    private static volatile bool running = true;
    private static readonly List<Person> people = new();

    static async Task Main()
    {
        Console.WriteLine("MQTT People Publisher");
        Console.WriteLine("=====================");
        Console.WriteLine("Sends 40 people initially, then updates age & status randomly\n");

        try
        {
            await ConnectToBroker();
            await SendInitialPeopleData();

            _ = Task.Run(UpdateLoopAsync); // fire and forget background updates

            Console.WriteLine("Commands:");
            Console.WriteLine("  [any key] → send random update now");
            Console.WriteLine("  q         → quit\n");

            while (running)
            {
                if (Console.KeyAvailable)
                {
                    var key = Console.ReadKey(true);
                    if (key.Key == ConsoleKey.Q)
                    {
                        running = false;
                        Console.WriteLine("\nShutting down...");
                    }
                    else
                    {
                        Console.WriteLine("→ Manual update");
                        await SendRandomUpdate();
                    }
                }

                await Task.Delay(80);
            }

            await Task.Delay(300); // give some time for last updates to fly
            await Cleanup();
            Console.WriteLine("Goodbye 👋");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Something went wrong: {ex.Message}");
        }
    }

    private static async Task ConnectToBroker()
    {
        var mqttFactory = new MqttFactory();
        client = mqttFactory.CreateMqttClient();

        var options = new MqttClientOptionsBuilder()
            .WithTcpServer("localhost", 1883)
            .WithClientId($"PeoplePub-{Guid.NewGuid().ToString("N")[..8]}")
            .WithCleanSession(true)
            .Build();

        await client.ConnectAsync(options);
        Console.WriteLine("Connected to localhost:1883 ✓");
    }

    private static async Task SendInitialPeopleData()
    {
        Console.WriteLine("Preparing and sending initial 40 people...");

        string[] firstNames = ["James", "Emma", "Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia",
            "Lucas", "Mia", "Alexander", "Charlotte", "Benjamin", "Amelia", "William", "Harper",
            "Michael", "Evelyn", "Daniel", "Abigail", "Henry", "Emily", "Jackson", "Elizabeth",
            "Sebastian", "Sofia", "Jack", "Avery", "Owen", "Ella", "Theodore", "Scarlett",
            "Aiden", "Grace", "Samuel", "Chloe", "Joseph", "Victoria", "John", "Madison",
            "David", "Luna", "Wyatt", "Layla"];

        string[] lastNames = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
            "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson",
            "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Thompson",
            "White", "Harris", "Clark", "Lewis", "Robinson", "Walker", "Hall", "Allen",
            "Young", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
            "Green", "Adams", "Nelson", "Baker"];

        var random = new Random(42); // reproducible results

        for (int i = 1; i <= 40; i++)
        {
            var fn = firstNames[(i - 1) % firstNames.Length];
            var ln = lastNames[(i - 1) % lastNames.Length];

            people.Add(new Person
            {
                Id = i,
                FirstName = fn,
                LastName = ln,
                Email = $"{fn.ToLower()}.{ln.ToLower()}@techcorp.com",
                Age = random.Next(22, 65),
                IsActive = random.NextDouble() > 0.3
            });
        }

        var payload = JsonSerializer.Serialize(people);
        var message = new MqttApplicationMessageBuilder()
            .WithTopic("app/people/data")
            .WithPayload(payload)
            .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
            .Build();

        await client!.PublishAsync(message);

        Console.WriteLine($"Sent {people.Count} people ({payload.Length / 1024.0:F1} KB)");
        Console.WriteLine($"  Example: {people[0].FirstName} {people[0].LastName} ({people[0].Email})");
    }

    private static async Task UpdateLoopAsync()
    {
        await Task.Delay(2000); // little warm-up delay

        int batchCount = 0;
        while (running)
        {
            batchCount++;
            Console.WriteLine($"\nBatch #{batchCount}");

            int updatesThisBatch = Random.Shared.Next(3, 6);
            for (int i = 0; i < updatesThisBatch; i++)
            {
                await SendRandomUpdate();
                await Task.Delay(100);
            }

            await Task.Delay(2000); // ~2s between batches
        }
    }

    private static async Task SendRandomUpdate()
    {
        if (client?.IsConnected != true) return;

        try
        {
            var rnd = Random.Shared;
            int id = rnd.Next(1, 41);

            var update = new PersonUpdate
            {
                Id = id,
                Age = rnd.Next(22, 65),
                IsActive = rnd.NextDouble() > 0.35
            };

            var payload = JsonSerializer.Serialize(update);

            var msg = new MqttApplicationMessageBuilder()
                .WithTopic("app/people/update")
                .WithPayload(payload)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await client.PublishAsync(msg);

            var person = people.Find(p => p.Id == id);
            string name = person != null ? $"{person.FirstName} {person.LastName}" : $"#{id}";

            Console.WriteLine($"  → {name,-18}  age: {update.Age,-2}  {(update.IsActive ? "✓" : "✗")}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Publish failed: {ex.Message}");
        }
    }

    private static async Task Cleanup()
    {
        if (client?.IsConnected == true)
        {
            await client.DisconnectAsync();
            Console.WriteLine("Disconnected cleanly");
        }
        client?.Dispose();
    }
}