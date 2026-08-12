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

## Coding legibility

Ensure the code is legible. Add newlines before and after control structures and definitions to improve code readability and maintainability.