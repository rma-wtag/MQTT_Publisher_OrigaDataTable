using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;

class Program
{
    private class Person
    {
        public int Id { get; set; }
        public string? FirstName { get; set; }
        public string? LastName { get; set; }
        public string? Email { get; set; }
        public int Age { get; set; }
        public bool IsActive { get; set; }
    }

    private static IMqttClient? client;
    private static volatile bool keepRunning = true;

    static async Task Main(string[] args)
    {
        Console.WriteLine("Starting people data publisher...");
        Console.WriteLine("Will send updates every ~5 seconds. Press Q to quit.\n");

        try
        {
            await Connect();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Couldn't connect: {ex.Message}");
            return;
        }

        var publishingTask = Task.Run(PublishLoop);

        Console.WriteLine("Press any key to send data right now, Q to stop");

        while (keepRunning)
        {
            if (Console.KeyAvailable)
            {
                var key = Console.ReadKey(intercept: true);
                if (key.Key == ConsoleKey.Q)
                {
                    keepRunning = false;
                    Console.WriteLine("\nStopping...");
                }
                else
                {
                    Console.WriteLine("→ Manual publish triggered");
                    await SendData();
                }
            }

            await Task.Delay(80); // small sleep so we don't hammer the CPU
        }

        await publishingTask;
        await Cleanup();

        Console.WriteLine("Done.");
    }

    private static async Task Connect()
    {
        var factory = new MqttFactory();
        client = factory.CreateMqttClient();

        var options = new MqttClientOptionsBuilder()
            .WithTcpServer("localhost", 1883)
            .WithClientId("PeoplePub-" + Guid.NewGuid().ToString("N")[..8])
            .WithCleanSession(true)
            .Build();

        await client.ConnectAsync(options);
        Console.WriteLine("Connected to broker.");
    }

    private static async Task Cleanup()
    {
        if (client?.IsConnected == true)
        {
            await client.DisconnectAsync();
            Console.WriteLine("Disconnected cleanly.");
        }
        client?.Dispose();
    }

    private static async Task PublishLoop()
    {
        int counter = 0;

        while (keepRunning)
        {
            counter++;
            Console.WriteLine($"[#{counter}] Sending update...");
            await SendData();

            try
            {
                await Task.Delay(5000, CancellationToken.None);
            }
            catch (TaskCanceledException) { }
        }
    }

    private static async Task SendData()
    {
        if (client?.IsConnected != true)
        {
            Console.WriteLine("Not connected - skipping publish");
            return;
        }

        try
        {
            var people = GenerateRandomPeople(80 + Random.Shared.Next(40)); // 80–120 people
            var payload = JsonSerializer.Serialize(people, new JsonSerializerOptions { WriteIndented = false });

            var msg = new MqttApplicationMessageBuilder()
                .WithTopic("app/people/data")
                .WithPayload(payload)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await client.PublishAsync(msg);

            Console.WriteLine($"Sent {people.Count} records  ({payload.Length / 1024:F1} kB)  {DateTime.Now:HH:mm:ss}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Publish failed: {ex.Message}");
        }
    }

    private static List<Person> GenerateRandomPeople(int count)
    {
        var list = new List<Person>(count);
        var rnd = Random.Shared;

        string[] first = ["James", "Emma", "Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia", "Lucas", "Mia",
                          "Alexander", "Charlotte", "Benjamin", "Amelia", "William", "Harper", "Michael", "Evelyn"];
        string[] last = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
                         "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore"];

        for (int i = 1; i <= count; i++)
        {
            var fn = first[rnd.Next(first.Length)];
            var ln = last[rnd.Next(last.Length)];

            list.Add(new Person
            {
                Id = i,
                FirstName = fn,
                LastName = ln,
                Email = $"{fn.ToLower()}.{ln.ToLower()}{i}@mail.example.com",
                Age = rnd.Next(19, 68),
                IsActive = rnd.NextDouble() > 0.35
            });
        }

        return list;
    }
}