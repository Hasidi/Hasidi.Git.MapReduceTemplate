using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Unity;
using Unity.Injection;

namespace MapReduceTemplate
{
    public static class UnityHandler
    {
        private static readonly IUnityContainer _unityContainer = new UnityContainer();

        static UnityHandler()
        {
            _unityContainer.RegisterSingleton<ILogger, ConsoleLogger>();

            var blockingCollection = new BlockingCollection<string>();

            _unityContainer.RegisterType<IDataReader<string>, FileDataReader>();

            _unityContainer
                .RegisterType<IDataParser<string>, StringDataParser>(
                    new InjectionConstructor(','));

            KeyValuePair<int, double> MapFunc(string[] x) 
                => new KeyValuePair<int, double>(Convert.ToInt32(x[1]), Convert.ToDouble(x[2]));

            _unityContainer.RegisterInstance("MapFunc", (Func<string[], KeyValuePair<int, double>>)MapFunc);

            _unityContainer.RegisterType<IMapper<int, double>, Mapper<string, int, double>>(
                new InjectionConstructor(
                    blockingCollection,
                    GetInstance<IDataParser<string>>(),
                    _unityContainer.Resolve(typeof(Func<string[], KeyValuePair<int, double>>), "MapFunc")));

            int ShuffleFunc(int key, int numReducers) 
                => key % numReducers;

            _unityContainer.RegisterType<IShuffler<int, double, double>, Shuffler<int, double, double>>(
                new InjectionConstructor(
                    (Func<int, int, int>) ShuffleFunc)
                );

            //double ReducersFunc(IEnumerable<double> values)
            //{
            //    if (values == null) throw new ArgumentNullException(nameof(values));
            //    return values.Average();
            //}

            double ReducersFunc(IEnumerable<double> values)
            {
                if (values == null) throw new ArgumentNullException(nameof(values));
                return values.Count();
            }

            _unityContainer.RegisterInstance("ReduceFunc", (Func<IEnumerable<double>, double>)ReducersFunc);
            

            _unityContainer.RegisterType<IReducer<int, double, double>, Reducer<int, double, double>>(
                new InjectionConstructor(
                    (Func<IEnumerable<double>, double>)ReducersFunc)
            );

            _unityContainer
                .RegisterType<IComponentsFactory, ComponentsFactory>();

            _unityContainer.RegisterType<MapReduceManager<string, int, double, double>>(
                new InjectionConstructor(
                    @"Files\ratings.csv", 3, 3, blockingCollection,
                        _unityContainer.Resolve<IComponentsFactory>(),
                        _unityContainer.Resolve<IDataReader<string>>())
            );

        }

        public static T GetInstance<T>()
        {
            return _unityContainer.Resolve<T>();
        }


    }
}
