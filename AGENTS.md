# Coding & Style Guidelines

## Multiline arguments

When dealing with a `function` or `method` with 3 or more arguments, split them into multiple lines.

```go
// Hard to read and maintain.
func foo(arg1 string, arg2 string, arg3 string, arg4 string) {
    // ...
}

// More legible and easier to maintain.
func foo(
    arg1 string,
    arg2 string, 
    arg3 string, 
    arg4 string,
) {
    // ... 
}
```

### Exception: When logging organize the arguments as "key,value"

```go
// Much harder to read and maintain. Each key,value should be on its own line
slog.Error(
    "error upserting resource",
    "cluster",
    cs.clusterCfg.Name,
    "namespace",
    u.GetNamespace(),
    "name",
    u.GetName(),
    "resource",
    gvr.Resource,
    "error",
    err,
)

// Easier to read and maintain. Not only each key,value is on its own line, but also the key,value pairs are grouped by resource name, namespace, etc.
slog.Error(
    "error upserting resource",
    "cluster", cs.clusterCfg.Name,
    "namespace", u.GetNamespace(),
    "name", u.GetName(),
    "resource", gvr.Resource,
    "err", err, // Always use "err" for the key that contains the actual error value.
)
```

## Coding legibility

Ensure the code is legible. Add newlines before and after control structures and definitions to improve code readability and maintainability.