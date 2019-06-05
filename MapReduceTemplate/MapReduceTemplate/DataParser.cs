using System;

namespace MapReduceTemplate
{
    public interface IDataParser<T>
    {
        T[] ParseRecord(T data);
    }

    public class StringDataParser : IDataParser<string>
    {
        private readonly char _delimiter;

        private static readonly ILogger Logger = UnityHandler.GetInstance<ILogger>();

        public StringDataParser(char delimiter)
        {
            _delimiter = delimiter;
        }

        public string[] ParseRecord(string dataStr)
        {
            try
            {
                var res = dataStr.Split(_delimiter);
                return res;
            }
            catch (Exception e)
            {
                Logger.LogError($"failed to parse [{dataStr}]", e);
                throw;
            }
        }
    }
}
