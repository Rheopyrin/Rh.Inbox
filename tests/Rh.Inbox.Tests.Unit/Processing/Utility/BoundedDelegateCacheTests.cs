using FluentAssertions;
using Rh.Inbox.Processing.Utility;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Processing.Utility;

public class BoundedDelegateCacheTests
{
    // Non-generic delegate that matches the pattern used in actual strategies
    private delegate Task ProcessDelegate(object value);

    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidParameters_CreatesInstance()
    {
        var target = new TestTarget();

        var cache = new BoundedDelegateCache<ProcessDelegate>(target, TestTarget.MethodName);

        cache.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithInvalidMethodName_ThrowsArgumentException()
    {
        var target = new TestTarget();

        var act = () => new BoundedDelegateCache<ProcessDelegate>(target, "NonExistentMethod");

        act.Should().Throw<ArgumentException>()
            .WithMessage("*'NonExistentMethod' not found*");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void Constructor_WithInvalidMaxSize_ThrowsArgumentOutOfRangeException(int maxSize)
    {
        var target = new TestTarget();

        var act = () => new BoundedDelegateCache<ProcessDelegate>(target, TestTarget.MethodName, maxSize: maxSize);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(256)]
    public void Constructor_WithValidMaxSize_CreatesInstance(int maxSize)
    {
        var target = new TestTarget();

        var cache = new BoundedDelegateCache<ProcessDelegate>(target, TestTarget.MethodName, maxSize: maxSize);

        cache.Should().NotBeNull();
    }

    #endregion

    #region GetOrAdd Tests

    [Fact]
    public void GetOrAdd_FirstCall_CreatesDelegateAndReturnsIt()
    {
        var target = new TestTarget();
        var cache = new BoundedDelegateCache<ProcessDelegate>(target, TestTarget.MethodName);

        var result = cache.GetOrAdd(typeof(string));

        result.Should().NotBeNull();
    }

    [Fact]
    public void GetOrAdd_SameType_ReturnsSameDelegate()
    {
        var target = new TestTarget();
        var cache = new BoundedDelegateCache<ProcessDelegate>(target, TestTarget.MethodName);

        var first = cache.GetOrAdd(typeof(string));
        var second = cache.GetOrAdd(typeof(string));

        first.Should().BeSameAs(second);
    }

    [Fact]
    public void GetOrAdd_DifferentTypes_ReturnsDifferentDelegates()
    {
        var target = new TestTarget();
        var cache = new BoundedDelegateCache<ProcessDelegate>(target, TestTarget.MethodName);

        var stringDelegate = cache.GetOrAdd(typeof(string));
        var intDelegate = cache.GetOrAdd(typeof(int));

        stringDelegate.Should().NotBeSameAs(intDelegate);
    }

    [Fact]
    public async Task GetOrAdd_ReturnedDelegate_CanBeInvoked()
    {
        var target = new TestTarget();
        var cache = new BoundedDelegateCache<ProcessDelegate>(target, TestTarget.MethodName);

        var processDelegate = cache.GetOrAdd(typeof(string));
        await processDelegate("test-value");

        target.LastProcessedValue.Should().Be("test-value");
        target.LastProcessedType.Should().Be(typeof(string));
    }

    #endregion

    #region LRU Eviction Tests

    [Fact]
    public void GetOrAdd_ExceedsMaxSize_EvictsOldestEntry()
    {
        var target = new TestTarget();
        var cache = new BoundedDelegateCache<ProcessDelegate>(target, TestTarget.MethodName, maxSize: 2);

        var type1Delegate = cache.GetOrAdd(typeof(Type1));
        var type2Delegate = cache.GetOrAdd(typeof(Type2));
        var type3Delegate = cache.GetOrAdd(typeof(Type3)); // Should evict Type1

        // Type1 should be evicted, so getting it again should create a new delegate
        var type1DelegateAgain = cache.GetOrAdd(typeof(Type1));

        type1DelegateAgain.Should().NotBeSameAs(type1Delegate, "Type1 was evicted and recreated");

        // Type2 and Type3 should still be cached (Type2 was accessed before Type3, but Type3 pushed Type1 out)
        var type3DelegateAgain = cache.GetOrAdd(typeof(Type3));
        type3DelegateAgain.Should().BeSameAs(type3Delegate, "Type3 should still be cached");
    }

    [Fact]
    public void GetOrAdd_AccessingExistingEntry_MovesToFrontOfLru()
    {
        var target = new TestTarget();
        var cache = new BoundedDelegateCache<ProcessDelegate>(target, TestTarget.MethodName, maxSize: 2);

        var type1Delegate = cache.GetOrAdd(typeof(Type1));
        var type2Delegate = cache.GetOrAdd(typeof(Type2));

        // Access Type1 again to move it to front of LRU
        cache.GetOrAdd(typeof(Type1));

        // Now add Type3 - should evict Type2 (oldest) instead of Type1
        cache.GetOrAdd(typeof(Type3));

        // Type1 should still be cached
        var type1DelegateAgain = cache.GetOrAdd(typeof(Type1));
        type1DelegateAgain.Should().BeSameAs(type1Delegate, "Type1 was recently accessed, should not be evicted");

        // Type2 should be evicted
        var type2DelegateAgain = cache.GetOrAdd(typeof(Type2));
        type2DelegateAgain.Should().NotBeSameAs(type2Delegate, "Type2 was evicted");
    }

    [Fact]
    public void GetOrAdd_MaxSizeOne_OnlyKeepsLastEntry()
    {
        var target = new TestTarget();
        var cache = new BoundedDelegateCache<ProcessDelegate>(target, TestTarget.MethodName, maxSize: 1);

        var type1Delegate = cache.GetOrAdd(typeof(Type1));
        var type2Delegate = cache.GetOrAdd(typeof(Type2)); // Evicts Type1

        // Type1 should be evicted
        var type1DelegateAgain = cache.GetOrAdd(typeof(Type1)); // Evicts Type2
        type1DelegateAgain.Should().NotBeSameAs(type1Delegate);

        // Type2 should be evicted
        var type2DelegateAgain = cache.GetOrAdd(typeof(Type2));
        type2DelegateAgain.Should().NotBeSameAs(type2Delegate);
    }

    #endregion

    #region Thread Safety Tests

    [Fact]
    public async Task GetOrAdd_ConcurrentAccess_ThreadSafe()
    {
        var target = new TestTarget();
        var cache = new BoundedDelegateCache<ProcessDelegate>(target, TestTarget.MethodName, maxSize: 10);

        var types = new[] { typeof(Type1), typeof(Type2), typeof(Type3), typeof(Type4), typeof(Type5) };
        var tasks = new List<Task>();

        for (int i = 0; i < 100; i++)
        {
            var typeIndex = i % types.Length;
            tasks.Add(Task.Run(() => cache.GetOrAdd(types[typeIndex])));
        }

        await Task.WhenAll(tasks);

        // Verify cache is still consistent - same type returns same delegate
        var delegate1 = cache.GetOrAdd(typeof(Type1));
        var delegate1Again = cache.GetOrAdd(typeof(Type1));
        delegate1.Should().BeSameAs(delegate1Again);
    }

    [Fact]
    public async Task GetOrAdd_ConcurrentAccessWithEviction_ThreadSafe()
    {
        var target = new TestTarget();
        var cache = new BoundedDelegateCache<ProcessDelegate>(target, TestTarget.MethodName, maxSize: 3);

        var types = new[] { typeof(Type1), typeof(Type2), typeof(Type3), typeof(Type4), typeof(Type5) };
        var tasks = new List<Task>();

        for (int i = 0; i < 1000; i++)
        {
            var typeIndex = i % types.Length;
            tasks.Add(Task.Run(() => cache.GetOrAdd(types[typeIndex])));
        }

        // Should not throw any exceptions
        var act = async () => await Task.WhenAll(tasks);
        await act.Should().NotThrowAsync();
    }

    #endregion

    #region Test Helpers

    private class TestTarget
    {
        public const string MethodName = "ProcessAsync";

        public object? LastProcessedValue { get; private set; }
        public Type? LastProcessedType { get; private set; }

        // Generic method that will be specialized for different types
        // The delegate signature is: Task ProcessDelegate(object value)
        // This matches the pattern: ProcessAsync<T> is called with T = messageType
        internal Task ProcessAsync<T>(object value)
        {
            LastProcessedValue = value;
            LastProcessedType = typeof(T);
            return Task.CompletedTask;
        }
    }

    // Dummy types for testing different type keys
    private class Type1 { }
    private class Type2 { }
    private class Type3 { }
    private class Type4 { }
    private class Type5 { }

    #endregion
}
