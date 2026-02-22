---
sidebar_position: 10
---

# Error handling

In ZenBPM, in addition to idiomatic Go error handling, we need to return correct HTTP status codes from our exposed REST API.
Often, the error handling happens deep in lower layers of the backend, including behind gRPC calls.
In REST controllers, we often lack the information to decide whether to return `NOT_FOUND` (404), `CLUSTER_ERROR` (502), or something else.
To solve this, we created a simple error composition to encapsulate an error code. It also implements the `error` interface:
```go
type ZenError struct {
    Code ZenErrorCode
    err  error
}

func (zenError *ZenError) Error() string {
    return zenError.err.Error()
}
```

There already exists functions that create certain type of the errors, for example:
```go
func ClusterError(err error) *ZenError {
	return &ZenError{ClusterErrorCode, err}
}

func NotFound(err error) *ZenError {
	return &ZenError{NotFoundCode, err}
}

// etc
```

So if you want to create a new error that would be mapped to some yet unsupported HTTP status code you can just add a new `ZenErrorCode`
and a corresponding function that creates that error.

Then in the code you would create such error, for example as:
```go
err := zenerr.NotFound(fmt.Errorf("failed to find process instance %d", req.GetProcessInstanceKey()))
return nil, err
```

If you need to serialize and send the error via GRPC then you can simply call `err.ToProtoError()`:
```go
if engine == nil {
    err := zenerr.ClusterError(fmt.Errorf("engine with partition %d was not found", partitionId))
	return &proto.GetProcessInstanceResponse{
	    Error: err.ToProtoError(),
    }, err
}
```

From the other side where you make the GRPC call you can wrap the GRPC `Error` to `ZenError`:
```go
if resp != nil && resp.Error != nil {
    e := fmt.Errorf("failed to get process instance from partition %d", partitionId)
    return nil, nil, zenerr.ToZenError(resp.Error, e)
}
```

Finally that's how you can decide what REST error response to return based on the particular `ZenError`:
```go
instance, activeElementInstances, err := s.node.GetProcessInstance(ctx, request.ProcessInstanceKey)
if err != nil {
	var zerr *zenerr.ZenError
	if errors.As(err, &zerr) {
        switch zerr.Code {
        case zenerr.ClusterErrorCode:
            return public.GetProcessInstance502JSONResponse(zerr.ToApiError()), nil
        case zenerr.NotFoundCode:
            return public.GetProcessInstance404JSONResponse(zerr.ToApiError()), nil
        default:
            return public.GetProcessInstance500JSONResponse(zerr.ToApiError()), nil
        }
    }
    return public.GetProcessInstance500JSONResponse(zenerr.TechnicalError(err).ToApiError()), nil
}
```