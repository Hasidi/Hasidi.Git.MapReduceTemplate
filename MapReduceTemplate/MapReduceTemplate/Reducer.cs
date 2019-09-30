using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MapReduceTemplate
{
    public interface IReducer<TKey, TValue, TOut>
    {
        void Feed(TKey key, List<TValue> values);
        SortedDictionary<TKey, TOut> StartReducing();
        SortedDictionary<TKey, TOut> GetData();
        Dictionary<TKey, TOut> SyncReducing(Dictionary<TKey, List<TValue>> toReduceCollection);
    }

    public class Reducer<TKey, TValue, TOut> : IReducer<TKey, TValue, TOut>
    {
        private readonly Func<IEnumerable<TValue>, TOut> _reduceFunc;
        private readonly object _dataLock;
        private readonly SortedDictionary<TKey, List<TValue>> _reducerBuffer;
        private readonly SortedDictionary<TKey, TOut> _reducedData;

        private static int nReducers;
        private readonly int _reducerId;

        private static readonly ILogger Logger = UnityHandler.GetInstance<ILogger>();

        public Reducer(Func<IEnumerable<TValue>, TOut> reduceFunc)
        {
            _reduceFunc = reduceFunc;
            _reducerId = nReducers;
            Interlocked.Add(ref nReducers, 1);
            _dataLock = new object();
            _reducerBuffer = new SortedDictionary<TKey, List<TValue>>();
            _reducedData = new SortedDictionary<TKey, TOut>();
        }


        public void Feed(TKey key, List<TValue> values)
        {
            lock (_dataLock)
            {
                if (_reducerBuffer.ContainsKey(key))
                    _reducerBuffer[key] = _reducerBuffer[key].Concat(values).ToList();
                else
                    _reducerBuffer[key] = values;
            }
        }

        public SortedDictionary<TKey, TOut> StartReducing()
        {
            //return Task.Run(() =>
            //{
                Logger.LogInfo($"---Reducer[{_reducerId}] has started work");
                foreach (var keyValues in _reducerBuffer)
                {
                    var reducedValue = _reduceFunc.Invoke(keyValues.Value);
                    _reducedData.Add(keyValues.Key, reducedValue);
                    Logger.LogDebug($"Reducer[{_reducerId}] has added key");
                }
                Logger.LogInfo($"Reducer[{_reducerId}] has finished work");
                return _reducedData;
            //});
        }

        public SortedDictionary<TKey, TOut> GetData()
        {
            var finishedWork = _reducedData.Keys.Count == _reducerBuffer.Keys.Count;
            return finishedWork ? _reducedData : null;
        }

        public Dictionary<TKey, TOut> SyncReducing(Dictionary<TKey, List<TValue>> toReduceCollection)
        {
            var res = new Dictionary<TKey, TOut>();
            foreach (var pair in toReduceCollection)
            {
                res[pair.Key] = _reduceFunc.Invoke(pair.Value);
            }
            

            return res;
        }
    }
}
