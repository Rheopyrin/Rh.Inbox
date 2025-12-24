using System.Reflection;

namespace Rh.Inbox.Processing.Utility;

/// <summary>
/// Thread-safe bounded cache for compiled generic method delegates with LRU eviction.
/// </summary>
internal sealed class BoundedDelegateCache<TDelegate> where TDelegate : Delegate
{
    private const int DefaultMaxCacheSize = 256;

    private readonly Dictionary<Type, LinkedListNode<(Type Key, TDelegate Value)>> _cache = new();
    private readonly LinkedList<(Type Key, TDelegate Value)> _lruList = new();
    private readonly MethodInfo _methodInfo;
    private readonly object _target;
    private readonly object _lock = new();
    private readonly int _maxSize;

    public BoundedDelegateCache(
        object target,
        string methodName,
        BindingFlags bindingFlags = BindingFlags.NonPublic | BindingFlags.Instance,
        int maxSize = DefaultMaxCacheSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxSize, 1);

        _maxSize = maxSize;
        _target = target;
        _methodInfo = target.GetType().GetMethod(methodName, bindingFlags)
            ?? throw new ArgumentException($"Method '{methodName}' not found on type '{target.GetType().Name}'", nameof(methodName));
    }

    public TDelegate GetOrAdd(Type messageType)
    {
        lock (_lock)
        {
            if (_cache.TryGetValue(messageType, out var node))
            {
                _lruList.Remove(node);
                var movedNode = _lruList.AddFirst(node.Value);
                _cache[messageType] = movedNode;
                return movedNode.Value.Value;
            }

            var newDelegate = CreateDelegate(messageType);
            var newNode = _lruList.AddFirst((messageType, newDelegate));
            _cache[messageType] = newNode;

            while (_cache.Count > _maxSize)
            {
                var oldest = _lruList.Last!;
                _lruList.RemoveLast();
                _cache.Remove(oldest.Value.Key);
            }

            return newDelegate;
        }
    }

    private TDelegate CreateDelegate(Type messageType)
    {
        var genericMethod = _methodInfo.MakeGenericMethod(messageType);
        return (TDelegate)Delegate.CreateDelegate(typeof(TDelegate), _target, genericMethod);
    }
}