namespace Rh.Inbox.Abstractions.Builders;

/// <summary>
/// Builder interface for configuring an inbox. Use the type selection methods to choose the inbox processing mode.
/// </summary>
public interface IInboxBuilder
{
    /// <summary>
    /// Configures the inbox as a default inbox that processes messages one at a time.
    /// </summary>
    /// <returns>A builder for further configuration of the default inbox.</returns>
    IDefaultInboxBuilder AsDefault();

    /// <summary>
    /// Configures the inbox as a batched inbox that processes messages in batches.
    /// </summary>
    /// <returns>A builder for further configuration of the batched inbox.</returns>
    IBatchedInboxBuilder AsBatched();

    /// <summary>
    /// Configures the inbox as a FIFO inbox that processes messages one at a time while maintaining order within groups.
    /// </summary>
    /// <returns>A builder for further configuration of the FIFO inbox.</returns>
    IFifoInboxBuilder AsFifo();

    /// <summary>
    /// Configures the inbox as a FIFO batched inbox that processes messages in batches while maintaining order within groups.
    /// </summary>
    /// <returns>A builder for further configuration of the FIFO batched inbox.</returns>
    IFifoBatchedInboxBuilder AsFifoBatched();
}
