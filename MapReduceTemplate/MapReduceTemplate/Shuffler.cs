using System;
using System.Collections.Generic;
using System.Linq;

namespace MapReduceTemplate
{
    public interface IShuffler<TKey, TValue, TOut>
    {
        void Shuffle(int mapperId, Dictionary<TKey, List<TValue>> dataBlock, IReducer<TKey, TValue, TOut>[] reducers);
    }

    public class Shuffler<TKey, TValue, TOut> : IShuffler<TKey, TValue, TOut>
    {
        /// <summary>
        /// Shuffle TKey to reducer number based on number of reducers
        /// </summary>
        private readonly Func<TKey, int, int> _shuffleFunc;

        private static readonly ILogger Logger = UnityHandler.GetInstance<ILogger>();

        public Shuffler(Func<TKey, int, int> shuffleFunc)
        {
            _shuffleFunc = shuffleFunc;
        }

        public void Shuffle(int mapperId, Dictionary<TKey, List<TValue>> dataBlock, IReducer<TKey, TValue, TOut>[] reducers)
        {
            try
            {
                Logger.LogInfo($"---Shuffler[{mapperId}] started work");
                dataBlock = dataBlock.OrderBy(key => key.Key)
                    .ToDictionary((keyItem) => keyItem.Key, (valueItem) => valueItem.Value);
                foreach (var keyValues in dataBlock)
                {
                    var reducerNum = _shuffleFunc.Invoke(keyValues.Key, reducers.Length);
                    Logger.LogDebug($"================================Shuffler[{mapperId}] has feed reducer[{reducerNum}]");
                    reducers[reducerNum].Feed(keyValues.Key, keyValues.Value);
                }
            }
            catch (Exception e)
            {
                Logger.LogError(e.Message);
                throw;
            }
            Logger.LogInfo($"======Shuffler[{mapperId}] finish working");
        }
    }
}
