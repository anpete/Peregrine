﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading.Tasks;
using Xunit;

namespace Peregrine.Tests
{
    public class PGSessionTests
    {
        private const string Host = "127.0.0.1";
        private const int Port = 5432;
        private const string Database = "aspnet5-Benchmarks";
        private const string User = "postgres";
        private const string Password = "Password1";

        [Fact]
        public async Task Start_success()
        {
            using (var session = new PGSession(Host, Port, Database, User, Password))
            {
                Assert.False(session.IsConnected);

                await session.StartAsync();

                Assert.True(session.IsConnected);
            }
        }

        [Fact]
        public async Task Start_timeout_on_bad_host()
        {
            using (var session = new PGSession("1.2.3.4", Port, Database, User, Password))
            {
                await Assert.ThrowsAsync<SocketException>(() => session.StartAsync());
            }
        }

        [Fact]
        public async Task Start_fail_on_bad_port()
        {
            using (var session = new PGSession(Host, 2345, Database, User, Password))
            {
                await Assert.ThrowsAnyAsync<SocketException>(() => session.StartAsync());
            }
        }

        [Fact]
        public async Task Start_fail_bad_user()
        {
            using (var session = new PGSession(Host, Port, Database, "Bad!", Password))
            {
                Assert.Equal(
                    "password authentication failed for user \"Bad!\"",
                    (await Assert.ThrowsAsync<InvalidOperationException>(
                        () => session.StartAsync())).Message);
            }
        }

        [Fact]
        public async Task Start_fail_bad_password()
        {
            using (var session = new PGSession(Host, Port, Database, User, "wrong"))
            {
                Assert.Equal(
                    "password authentication failed for user \"postgres\"",
                    (await Assert.ThrowsAsync<InvalidOperationException>(
                        () => session.StartAsync())).Message);
            }
        }

        [Fact]
        public async Task Dispose_when_open()
        {
            using (var session = new PGSession(Host, Port, Database, User, Password))
            {
                await session.StartAsync();

                Assert.True(session.IsConnected);

                session.Dispose();

                Assert.Throws<ObjectDisposedException>(() => session.IsConnected);
            }
        }

        [Fact]
        public async Task Terminate_when_open_reopen()
        {
            using (var session = new PGSession(Host, Port, Database, User, Password))
            {
                await session.StartAsync();

                Assert.True(session.IsConnected);

                session.Terminate();

                Assert.False(session.IsConnected);

                await session.StartAsync();

                Assert.True(session.IsConnected);
            }
        }

        [Fact]
        public async Task Prepare_success()
        {
            using (var session = new PGSession(Host, Port, Database, User, Password))
            {
                await session.StartAsync();

                await session.PrepareAsync("_p0", "select id, message from fortune");
            }
        }

        [Fact]
        public async Task Prepare_failure_invalid_sql()
        {
            using (var session = new PGSession(Host, Port, Database, User, Password))
            {
                await session.StartAsync();

                Assert.Equal(
                    "syntax error at or near \"boom\"",
                    (await Assert.ThrowsAsync<InvalidOperationException>(
                        () => session.PrepareAsync("_p0", "boom!"))).Message);
            }
        }

        [Fact]
        public async Task Execute_success()
        {
            using (var session = new PGSession(Host, Port, Database, User, Password))
            {
                await session.StartAsync();
                await session.PrepareAsync("_p0", "select id, message from fortune");

                var fortunes = new List<Fortune>();

                await session.ExecuteAsync(
                    "_p0",
                    () =>
                        {
                            var fortune = new Fortune();

                            fortunes.Add(fortune);

                            return fortune;
                        },
                    (f, c, l, b) =>
                        {
                            switch (c)
                            {
                                case 0:
                                    f.Id = b.ReadInt();
                                    break;
                                case 1:
                                    f.Message = b.ReadString(l);
                                    break;
                            }
                        });

                Assert.Equal(12, fortunes.Count);
            }
        }

        public class Fortune
        {
            public int Id { get; set; }
            public string Message { get; set; }
        }
    }
}
