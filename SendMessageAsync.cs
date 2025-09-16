public virtual Task<SendMessageResponse> SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default(CancellationToken))
{
    InvokeOptions invokeOptions = new InvokeOptions();
    invokeOptions.RequestMarshaller = SendMessageRequestMarshaller.Instance;
    invokeOptions.ResponseUnmarshaller = SendMessageResponseUnmarshaller.Instance;
    return InvokeAsync<SendMessageResponse>(request, invokeOptions, cancellationToken);
}

internal virtual SendMessageBatchResponse SendMessageBatch(SendMessageBatchRequest request)
{
    InvokeOptions invokeOptions = new InvokeOptions();
    invokeOptions.RequestMarshaller = SendMessageBatchRequestMarshaller.Instance;
    invokeOptions.ResponseUnmarshaller = SendMessageBatchResponseUnmarshaller.Instance;
    return Invoke<SendMessageBatchResponse>(request, invokeOptions);
}