using System;
using System.Collections.Generic;
using System.Text;
using StackExchange.Redis;

namespace Redis.Shared
{
    public class RedisService(string host, string port, string password)
    {
        private ConnectionMultiplexer? ConnectionMultiplexer { get; set; }

        private IDatabase? _db;
        private ISubscriber? _subscriber;

        public async Task Init()
        {
            ConnectionMultiplexer = await ConnectionMultiplexer.ConnectAsync($"{host}:{port},password={password}");


            _db = ConnectionMultiplexer.GetDatabase(0);

            _subscriber = ConnectionMultiplexer.GetSubscriber();
        }

        public IDatabase GetDatabase => _db!;

        public ISubscriber Subscriber => _subscriber!;
    }
}
