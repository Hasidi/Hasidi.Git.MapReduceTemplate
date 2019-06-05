using System;

namespace MapReduceTemplate
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("*** Starting App");
            //WriteDemoFile();

            var mapReduce = UnityHandler.GetInstance<MapReduceManager<string, int, double, double>>();

            var res = mapReduce.Run();
            //write results somewhere
            Console.WriteLine("*** App has finished");
        }
    }
}
