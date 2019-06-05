using System.Collections.Generic;
using System.IO;

namespace MapReduceTemplate
{
    public interface IDataReader<out T>
    {
        IEnumerable<T> ReadData(string filePath);
    }

    public class FileDataReader : IDataReader<string>
    {
        private static readonly ILogger Logger = UnityHandler.GetInstance<ILogger>();

        public IEnumerable<string> ReadData(string filePath)
        {
            using (var streamReader = new StreamReader(filePath))
            {
                while (streamReader.Peek() > 0)
                {
                    var line = streamReader.ReadLine();
                    yield return line;
                }
            }
            Logger.LogInfo("finished read all data");
        }
    }
}
