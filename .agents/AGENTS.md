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
