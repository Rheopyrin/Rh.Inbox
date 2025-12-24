using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Lifecycle;
using Rh.Inbox.Configuration;
using Rh.Inbox.Configuration.Registry;
using Rh.Inbox.Exceptions;
using Rh.Inbox.Inboxes;
using Rh.Inbox.Inboxes.Factory;
using Rh.Inbox.Processing;

namespace Rh.Inbox.Management;

internal sealed class InboxManager : IInboxManager, IInboxManagerInternal, IAsyncDisposable
{
    private readonly InboxConfigurationRegistry _configurationRegistry;
    private readonly IServiceProvider _serviceProvider;
    private readonly IInboxLifecycle _lifecycle;
    private readonly IEnumerable<IInboxLifecycleHook> _lifecycleHooks;
    private readonly IInboxFactory _inboxFactory;
    private readonly ILogger<InboxManager> _logger;

    private readonly ConcurrentDictionary<string, InboxBase> _inboxes = new();
    private readonly ConcurrentDictionary<string, InboxProcessingLoop> _processingLoops = new();
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);
    private bool _disposed;

    public InboxManager(
        InboxConfigurationRegistry configurationRegistry,
        IServiceProvider serviceProvider,
        IInboxLifecycle lifecycle,
        IEnumerable<IInboxLifecycleHook> lifecycleHooks,
        IInboxFactory inboxFactory,
        ILogger<InboxManager> logger)
    {
        _configurationRegistry = configurationRegistry;
        _serviceProvider = serviceProvider;
        _lifecycle = lifecycle;
        _lifecycleHooks = lifecycleHooks;
        _inboxFactory = inboxFactory;
        _logger = logger;

        InitializeInboxes();
    }

    public bool IsRunning => _lifecycle.IsRunning;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _lifecycleLock.WaitAsync(cancellationToken);
        try
        {
            if (_lifecycle.IsRunning)
            {
                return;
            }

            _lifecycle.Start();

            try
            {
                _logger.LogInformation("InboxManager starting...");

                foreach (var loop in _processingLoops.Values)
                {
                    await loop.StartAsync(cancellationToken);
                }

                await Task.WhenAll(_lifecycleHooks.Select(e => e.OnStart(cancellationToken)));
                _logger.LogInformation("InboxManager started. {Count} inbox(es) initialized.", _inboxes.Count);
            }
            catch (Exception e)
            {
                _lifecycle.Stop();
                await StopLifecycleHooks(cancellationToken);
                _logger.LogError(e, "Failed to start InboxManager.");
                throw;
            }
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _lifecycleLock.WaitAsync(cancellationToken);
        try
        {
            if (!_lifecycle.IsRunning)
            {
                return;
            }

            _logger.LogInformation("InboxManager stopping...");

            // Signal lifecycle to stop (triggers cleanup services)
            _lifecycle.Stop();

            // Stop processing loops
            var stopTasks = _processingLoops.Values
                .Select(loop => loop.StopAsync(cancellationToken))
                .ToArray();

            await Task.WhenAll(stopTasks);

            _logger.LogInformation("InboxManager stopped.");
        }
        finally
        {
            await StopLifecycleHooks(cancellationToken);
            _lifecycleLock.Release();
        }
    }

    private async Task StopLifecycleHooks(CancellationToken cancellationToken)
    {
        await Task.WhenAll(_lifecycleHooks.Select(async e =>
        {
            try
            {
                await e.OnStop(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in lifecycle hook {HookName}", e.GetType().Name);
            }
        }));
    }

    public IInbox GetInbox()
    {
        return GetInbox(InboxOptions.DefaultInboxName);
    }

    public IInbox GetInbox(string name)
    {
        if (!_inboxes.TryGetValue(name, out var inbox))
        {
            throw new InboxNotFoundException(name);
        }
        return inbox;
    }

    private void InitializeInboxes()
    {
        foreach (var configuration in _configurationRegistry.GetAll())
        {
            var inbox = _inboxFactory.Create(configuration);
            _inboxes[configuration.InboxName] = inbox;

            // Only create processing loop if there are registered message handlers
            if (configuration.MetadataRegistry.HasRegisteredMessages)
            {
                _processingLoops[configuration.InboxName] =
                    ActivatorUtilities.CreateInstance<InboxProcessingLoop>(_serviceProvider, inbox);
            }
        }
    }

    public InboxBase GetInboxInternal(string name)
    {
        if (!_inboxes.TryGetValue(name, out var inbox))
        {
            throw new InboxNotFoundException(name);
        }
        return inbox;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        // Stop gracefully if still running
        if (_lifecycle.IsRunning)
        {
            await StopAsync(CancellationToken.None);
        }

        foreach (var loop in _processingLoops.Values)
        {
            loop.Dispose();
        }

        foreach (var inbox in _inboxes.Values)
        {
            var storageProvider = inbox.GetStorageProvider();
            switch (storageProvider)
            {
                case IAsyncDisposable asyncDisposable:
                    await asyncDisposable.DisposeAsync();
                    break;
                case IDisposable disposable:
                    disposable.Dispose();
                    break;
            }
        }

        _lifecycleLock.Dispose();
        _disposed = true;
        GC.SuppressFinalize(this);
    }

}