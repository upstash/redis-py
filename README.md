# Important!

This branch's role is to provide a python 3.10-compatible version to allow the use
in platforms such as AWS Lambda.

For that to be possible, some typing features like the "Self" type and the Generic 
TypedDict were removed.

Currently, this is the only branch from which the sdk is released, and it holds the
version used by the rate-limit sdk too.