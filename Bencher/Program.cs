// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Peregrine;

#pragma warning disable 4014

namespace Bencher
{
    internal class Program
    {
        private const string Host = "127.0.0.1";
        private const int Port = 5432;
        private const string Database = "aspnet5-Benchmarks";
        private const string User = "postgres";
        private const string Password = "Password1";

        private const int NumTasks = 16;

        private static int _counter;
        private static bool _stopping;

        private static async Task Main()
        {
            var lastDisplay = DateTime.UtcNow;

            var tasks
                = Enumerable
                    .Range(1, NumTasks)
                    .Select(
                        _ => Task.Factory.StartNew(DoWorkAsync, TaskCreationOptions.LongRunning)
                            .Unwrap())
                    .ToList();

            Task.Run(
                async () =>
                    {
                        while (!_stopping)
                        {
                            await Task.Delay(1000);

                            var now = DateTime.UtcNow;
                            var tps = (int)(_counter / (now - lastDisplay).TotalSeconds);

                            Console.Write($"{tasks.Count} Threads, {tps} tps");

                            Console.CursorLeft = 0;

                            lastDisplay = now;

                            _counter = 0;
                        }
                    });

            Task.Run(
                () =>
                    {
                        Console.ReadLine();

                        _stopping = true;
                    });

            await Task.WhenAll(tasks);
        }

        private static async Task DoWorkAsync()
        {
            using (var session = new PGSession(Host, Port, Database, User, Password))
            {
                await session.StartAsync();
                await session.PrepareAsync("_p0", "select id, message from fortune");

                while (!_stopping)
                {
                    Interlocked.Increment(ref _counter);

                    await session.ExecuteAsync<object>(
                        "_p0",
                        null,
                        (f, c, l, b) =>
                            {
                                switch (c)
                                {
                                    case 0:
                                        b.ReadInt();
                                        break;
                                    case 1:
                                        b.ReadString(l);
                                        break;
                                }
                            });
                }
            }
        }
    }
}
