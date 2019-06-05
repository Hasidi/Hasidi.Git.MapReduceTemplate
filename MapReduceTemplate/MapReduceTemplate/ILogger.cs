
using System;

namespace MapReduceTemplate
{
    public interface ILogger
    {
        void LogInfo(string log);
        void LogDebug(string log);
        void LogError(string log, Exception e = null);
    }

    public class ConsoleLogger : ILogger
    {

        public void LogInfo(string log)
        {
            Console.WriteLine(log);
        }

        public void LogDebug(string log)
        {
        }

        public void LogError(string log, Exception e = null)
        {
            Console.WriteLine(e != null ? $"{log}. Exception Message: {e.Message}" : log);
        }
    }
}
