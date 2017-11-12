﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Peregrine
{
    public class PGSession : IDisposable
    {
        private const int DefaultConnectionTimeout = 2000; // ms

        private readonly string _host;
        private readonly int _port;
        private readonly string _database;
        private readonly string _user;
        private readonly string _password;

        private WriteBuffer _writeBuffer;
        private ReadBuffer _readBuffer;

        private AwaitableSocket _awaitableSocket;

        private bool _disposed;

        public PGSession(
            string host,
            int port,
            string database,
            string user,
            string password)
        {
            _host = host;
            _port = port;
            _database = database;
            _user = user;
            _password = password;
        }

        public bool IsConnected
        {
            get
            {
                ThrowIfDisposed();

                return _awaitableSocket?.IsConnected == true;
            }
        }

        public async Task PrepareAsync(string statementName, string query)
        {
            ThrowIfDisposed();
            ThrowIfNotConnected();

            await _writeBuffer
                .StartMessage('P')
                .WriteString(statementName)
                .WriteString(query)
                .WriteShort(0)
                .EndMessage()
                .StartMessage('S')
                .EndMessage()
                .FlushAsync();

            await _readBuffer.ReceiveAsync();

            var message = ReadMessage();

            switch (message.Type)
            {
                case MessageType.ParseComplete:
                    break;

                case MessageType.ErrorResponse:
                    throw new InvalidOperationException(ReadErrorMessage());

                default:
                    throw new NotImplementedException(message.Type.ToString());
            }
        }

        public async Task ExecuteAsync<TRow>(
            string statementName, 
            Func<TRow> rowFactory,
            Action<TRow, int, int, ReadBuffer> columnBinder)
        {
            ThrowIfDisposed();
            ThrowIfNotConnected();

            if (!IsConnected)
            {
                throw new InvalidOperationException();
            }

            await _writeBuffer
                .StartMessage('B')
                .WriteNull()
                .WriteString(statementName)
                .WriteShort(0)
                .WriteShort(0)
                .WriteShort(1)
                .WriteShort(1)
                .EndMessage()
                .StartMessage('E')
                .WriteNull()
                .WriteInt(0)
                .EndMessage()
                .StartMessage('S')
                .EndMessage()
                .FlushAsync();

            await _readBuffer.ReceiveAsync();

            read:

            var message = ReadMessage();

            switch (message.Type)
            {
                case MessageType.BindComplete:
                    goto read;

                case MessageType.DataRow:

                    //var row = rowFactory();

                    var columns = _readBuffer.ReadShort();

                    for (var i = 0; i < columns; i++)
                    {
                        var length = _readBuffer.ReadInt();

                        columnBinder(default, i, length, _readBuffer);
                    }

                    goto read;

                case MessageType.ErrorResponse:
                    throw new InvalidOperationException(ReadErrorMessage());

                case MessageType.CommandComplete:
                    return;

                default:
                    throw new NotImplementedException(message.Type.ToString());
            }
        }

        public Task StartAsync(int millisecondsTimeout = DefaultConnectionTimeout)
        {
            ThrowIfDisposed();

            return IsConnected
                ? Task.CompletedTask
                : StartSessionAsync(millisecondsTimeout);
        }

        private async Task StartSessionAsync(int millisecondsTimeout)
        {
            await OpenSocketAsync(millisecondsTimeout);

            _writeBuffer = new WriteBuffer(_awaitableSocket);
            _readBuffer = new ReadBuffer(_awaitableSocket);

            await WriteStartupAsync();

            await _readBuffer.ReceiveAsync();

            read:

            var message = ReadMessage();

            switch (message.Type)
            {
                case MessageType.AuthenticationRequest:
                {
                    var authenticationRequestType
                        = (AuthenticationRequestType)_readBuffer.ReadInt();

                    switch (authenticationRequestType)
                    {
                        case AuthenticationRequestType.AuthenticationOk:
                        {
                            return;
                        }

                        case AuthenticationRequestType.AuthenticationMD5Password:
                        {
                            var salt = _readBuffer.ReadBytes(4);
                            var hash = Hashing.CreateMD5(_password, _user, salt);

                            await _writeBuffer
                                .StartMessage('p')
                                .WriteBytes(hash)
                                .EndMessage()
                                .FlushAsync();

                            await _readBuffer.ReceiveAsync();

                            goto read;
                        }

                        default:
                            throw new NotImplementedException(authenticationRequestType.ToString());
                    }
                }

                case MessageType.BackendKeyData:
                case MessageType.EmptyQueryResponse:
                case MessageType.ErrorResponse:
                    throw new InvalidOperationException(ReadErrorMessage());

                case MessageType.ParameterStatus:
                case MessageType.ReadyForQuery:
                    throw new NotImplementedException($"Unhandled MessageType '{message.Type}'");

                default:
                    throw new InvalidOperationException($"Unexpected MessageType '{message.Type}'");
            }
        }

        private async Task OpenSocketAsync(int millisecondsTimeout)
        {
            var socket
                = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                {
                    NoDelay = true
                };

            _awaitableSocket
                = new AwaitableSocket(
                    new SocketAsyncEventArgs
                    {
                        RemoteEndPoint = new IPEndPoint(IPAddress.Parse(_host), _port)
                    },
                    socket);

            using (var cts = new CancellationTokenSource())
            {
                cts.CancelAfter(millisecondsTimeout);

                await _awaitableSocket.ConnectAsync(cts.Token);
            }
        }

        private (MessageType Type, int Length) ReadMessage()
        {
            var messageType = (MessageType)_readBuffer.ReadByte();
            var length = _readBuffer.ReadInt() - 4;

            return (messageType, length);
        }

        private string ReadErrorMessage()
        {
            string message = null;

            read:

            var code = (ErrorFieldTypeCode)_readBuffer.ReadByte();

            switch (code)
            {
                case ErrorFieldTypeCode.Done:
                    break;
                case ErrorFieldTypeCode.Message:
                    message = _readBuffer.ReadNullTerminatedString();
                    break;
                default:
                    _readBuffer.ReadNullTerminatedString();
                    goto read;
            }

            return message;
        }

        private async Task WriteStartupAsync()
        {
            const int protocolVersion3 = 3 << 16;

            var parameters = new(string Name, string Value)[]
            {
                ("user", _user),
                ("client_encoding", "UTF8"),
                ("database", _database)
            };

            _writeBuffer
                .StartMessage()
                .WriteInt(protocolVersion3);

            for (var i = 0; i < parameters.Length; i++)
            {
                var p = parameters[i];

                _writeBuffer
                    .WriteString(p.Name)
                    .WriteString(p.Value);
            }

            await _writeBuffer
                .WriteNull()
                .EndMessage()
                .FlushAsync();
        }

        public void Terminate()
        {
            ThrowIfDisposed();

            if (IsConnected)
            {
                try
                {
                    _writeBuffer
                        .StartMessage('X')
                        .EndMessage()
                        .FlushAsync()
                        .GetAwaiter()
                        .GetResult();
                }
                catch (SocketException)
                {
                    // Socket may have closed
                }
            }

            _awaitableSocket?.Dispose();
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                Terminate();

                _disposed = true;
            }
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(PGSession));
            }
        }

        private void ThrowIfNotConnected()
        {
            if (!IsConnected)
            {
                throw new InvalidOperationException();
            }
        }
    }
}
