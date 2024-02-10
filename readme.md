# pg-http-proxy

A proxy that provides a HTTP interface to a postgresql database.

Features:

* Connection pooling
* API key protection
* Selectable JSON or list output
* Selectable row and column separators for list output


# Security

This proxy should not be exposed to the internet.  It is meant to be run between a database in a private VPC and your API code runner.  You should never connect to it directly from a frontend client such as a web browser or mobile app.


# Default output

If unspecified, the default output is a list format with rows separated by ASCII character 30 (record separator)
and columns separated by ASCII character 31 (unit separator).  This will allow columns with multi row data.

List output will have the first row containing the column names.  It should never contain one row only


# Changing output options

The output can be changed by passing headers in the request.
If the `Accept` header is set to `application/json`  the output will be changed to JSON format
Passing the header `x-compact-json` with a value of `true` will alter the JSON output to return an array of arrays.
The first item in the array will contain the header names

The row and colum separators in the list output can be changed with the `x-rowsep` and `x-colsep` header options.
These two headers accept a number between 0 and 255 representing the code for a byte
