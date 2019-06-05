using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MapReduceTemplate
{
    public interface IMapper<TKey, TValue>
    {
        Task<int> StartMapping();
        Dictionary<TKey, List<TValue>> GetData();
        int GetId();
    }

    public class Mapper<TInput, TKey, TValue> : IMapper<TKey, TValue>
    {
        private readonly BlockingCollection<TInput> _blockingCollection;
        private readonly IDataParser<TInput> _dataParser;
        private readonly Func<TInput[], KeyValuePair<TKey, TValue>> _mapFunc;
        private readonly int _mapperId;

        private Dictionary<TKey, List<TValue>> _mappingData;

        private static readonly ILogger Logger = UnityHandler.GetInstance<ILogger>();
        private static int nMappers;
        private bool _isFinished;


        public Mapper
        (
            BlockingCollection<TInput> blockingCollection, 
            IDataParser<TInput> dataParser, 
            Func<TInput[], KeyValuePair<TKey, TValue>> mapFunc
        )
        {
            _blockingCollection = blockingCollection;
            _dataParser = dataParser;
            _mapFunc = mapFunc;
            _mapperId = nMappers; 
            Interlocked.Add(ref nMappers, 1);
        }

        public Task<int> StartMapping()
        {
            return Task.Run(() =>
            {
                _mappingData = new Dictionary<TKey, List<TValue>>();
                _isFinished = false;
                Logger.LogInfo($"---Mapper[{_mapperId}] started work");
                while (!_blockingCollection.IsCompleted)
                {
                    //if (_mapperId == 2 && _mappingData.Keys.Count > 5000)
                    //    break;
                    _blockingCollection.TryTake(out var record);
                    if (record == null)
                    {
                        Logger.LogDebug("Queue is busy or empty.. failed to take record");
                        Thread.Sleep(500);
                        continue;
                    }
                    Logger.LogDebug($"Mapper [{_mapperId}] took record");
                    try
                    {
                        var parsedData = _dataParser.ParseRecord(record);
                        var keyValuePair = _mapFunc(parsedData);
                        _AddToCollection(keyValuePair);
                    }
                    catch (Exception e)
                    {
                        Logger.LogError($"Failed to map record [{record}]", e);
                    }
                }
                _isFinished = true;
                Logger.LogInfo($"======Mapper[{_mapperId}] finish working");
                return _mapperId;
            });

        }

        private void _AddToCollection(KeyValuePair<TKey, TValue> keyValuePair)
        {
            if (!_mappingData.ContainsKey(keyValuePair.Key))
                _mappingData[keyValuePair.Key] = new List<TValue>();
            _mappingData[keyValuePair.Key].Add(keyValuePair.Value);
        }

        public Dictionary<TKey, List<TValue>> GetData() => _isFinished ? _mappingData : null;

        public int GetId() => _mapperId;
    }
}
