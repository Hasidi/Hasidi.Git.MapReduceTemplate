
using System.Linq;

namespace MapReduceTemplate
{
    public interface IComponentsFactory
    {
        T[] CreateComponents<T>(int nComponents);
    }

    public class ComponentsFactory : IComponentsFactory
    {
        public T[] CreateComponents<T>(int nComponents)
        {
            return Enumerable.Range(0, nComponents)
                .Select(_ => UnityHandler.GetInstance<T>())
                .ToArray();
        }
    }
}
