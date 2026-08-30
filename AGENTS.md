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

## Variables / Properties initialization

Always use inline literal initialization instead of `make()` as much as possible for slices and maps, unless a specific capacity is required for performance.

```go
// Harder to read
seen := make(map[string]bool)

// Better
seen := map[string]bool{}
```

Prefer using `any` instead of `interface{}` whenever possible for better readability.

## Sets / Seen Maps

When creating sets or "seen" maps, prefer using `map[T]bool` instead of `map[T]struct{}{}`. In modern Go, this is perfectly fine for performance and greatly improves code readability during checks (`if seen[key]` instead of `if _, exists := seen[key]; exists`).

## Interfaces

When defining a new Type and implementing it on a concrete type, always add a build-time interface check near the type definition. Ensure it is preceded by a comment.

```go
// Build-time interface check.
var _ datastore.Syncer = &PG{}
```

## Formatting

Always run `go fmt ./...` before wrapping up a plan's execution or concluding a coding task to ensure all Go files are properly formatted.