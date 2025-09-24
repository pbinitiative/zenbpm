
### Coding standards

The core developers of this project like to establish some guidelines and standards 
to ensure the code is consistent and easier to maintain.

#### Patterns

##### Options Pattern over Builder Patters

Both patterns solve the problem of having flexibility and variations in constructing an object.

Functional options is a pattern in which you declare an opaque Option type that records information in some internal struct.
You accept a variadic number of these options and act upon the full information recorded by the options on the internal struct.

Use this pattern for optional arguments in constructors and other public APIs that you foresee needing to expand,
especially if you already have three or more arguments on those functions.

Example code: https://github.com/uber-go/guide/blob/master/style.md#functional-options
