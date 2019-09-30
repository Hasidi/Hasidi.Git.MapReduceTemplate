using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MapReduceTemplate
{
    public class MapReduceManager<TIn, TKey, TValue, TOut>
    {
        private readonly string _dataFilePath;
        private readonly int _nMappers;
        private readonly int _nReducers;
        private readonly BlockingCollection<TIn> _blockingCollection;
        private readonly IComponentsFactory _componentsFactory;
        private readonly IDataReader<TIn> _dataReader;

        private IMapper<TKey, TValue>[] _mappers;
        private IShuffler<TKey, TValue, TOut>[] _shufflers;
        private IReducer<TKey, TValue, TOut>[] _reducers;

        private ConcurrentBag<IMapper<TKey, TValue>> _mappersList;
        private ConcurrentBag<IShuffler<TKey, TValue, TOut>> _shufflersList;
        private ConcurrentBag<IReducer<TKey, TValue, TOut>> _reducersList;


        private static readonly ILogger Logger = UnityHandler.GetInstance<ILogger>();

        public MapReduceManager
        (
            string dataFilePath, int nMappers, int nReducers, 
            BlockingCollection<TIn> blockingCollection,
            IComponentsFactory componentsFactory,
            IDataReader<TIn> dataReader
        )
        {
            _dataFilePath = dataFilePath;
            _nMappers = nMappers;
            _nReducers = nReducers;
            _blockingCollection = blockingCollection;
            _componentsFactory = componentsFactory;
            _dataReader = dataReader;
        }

        private void _Init()
        {
            _mappers = _componentsFactory.CreateComponents<IMapper<TKey, TValue>>(_nMappers);
            _shufflers = _componentsFactory.CreateComponents<IShuffler<TKey, TValue, TOut>>(_nMappers);
            _reducers = _componentsFactory.CreateComponents<IReducer<TKey, TValue, TOut>>(_nReducers);
        }


        public Dictionary<TKey, TOut> Run()
        {
            var watch = new Stopwatch();
            _Init();

            watch.Restart();
            Task.Run(_SplitData);
            Thread.Sleep(2000);
            //_SplitData();
            _RunMappers();
            Logger.LogInfo("mappers and shufflers have finished their work....");
            var res = _RunReducers();
            watch.Stop();

            Logger.LogInfo($"total running time = [{watch.ElapsedMilliseconds}]");
            return res;
        }

        private void _SplitData()
        {
            var data = _dataReader.ReadData(_dataFilePath);
            foreach (var rec in data)
            {
                _blockingCollection.Add(rec);
            }
            _blockingCollection.CompleteAdding();
        }

        private void _RunMappers()
        {

            Parallel.ForEach(_mappers, (mapper) =>
            {
                Logger.LogInfo($"mapper[{mapper.GetId()}] function was invoked");
                var mapperId = mapper.StartMapping();
                var data = _mappers[mapperId].GetData();
                _shufflers[mapperId].Shuffle(mapperId, data, _reducers);
            });

            //var tasks = new List<Task>();
            //foreach (var mapper in _mappers)
            //{
            //    Logger.LogInfo($"mapper[{mapper.GetId()}] function was invoked");
            //    var mapperTask = mapper.StartMapping();
            //    //tasks.Add(mapperTask);
            //    var shuffleTask = mapperTask.ContinueWith( (mapRes) =>
            //    {
            //        var mapperId = mapRes.Result;
            //        var data = _mappers[mapperId].GetData();
            //        //_shufflers[mapperId].Shuffle(mapperId, data, _reducers).Wait();
            //        _shufflers[mapperId].Shuffle(mapperId, data, _reducers);
            //    });
            //    tasks.Add(shuffleTask);
            //}
            //Task.WaitAll(tasks.ToArray());
        }

        private Dictionary<TKey, TOut> _RunReducers()
        {
            Parallel.ForEach(_reducers, (reducer) => { reducer.StartReducing(); });

            //var tasks = _reducers.AsParallel().Select(r => r.StartReducing());
            //Task.WaitAll(tasks.ToArray());

            //concat results
            var res = _reducers.SelectMany(x => x.GetData()).ToDictionary(x => x.Key, x => x.Value);
            return res;
        }
    }
}
