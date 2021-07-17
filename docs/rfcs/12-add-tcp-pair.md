- Author: bokket  [bokkett@gmail.com](mailto:bokkett@gmail.com)
- Start Date: 2021-07-17
- RFC PR: [beyondstorage/go-service-hdfs#12](https://github.com/beyondstorage/go-service-hdfs/pull/12)
- Tracking Issue: [https://github.com/beyondstorage/go-endpoint/issues/7](https://github.com/beyondstorage/go-endpoint/issues/7)

# RFC-11: Add Tcp pair

Releated issue: [https://github.com/beyondstorage/go-endpoint/issues/7](https://github.com/beyondstorage/go-endpoint/issues/7)

## Background

hdfs usually use the `New(address string)` method to access a namenode node, the user will be the user running the code. If the address is an empty string, it will try to get the NameNode address from the Hadoop configuration file.

## Proposal

I suggest adding a pair to allow the user to specify the address.

- The `type` of `tcp` should be `String` and is a `const`
- The `format` of `ProtocolTcp` should follow  [go-endpoint](https://github.com/beyondstorage/go-endpoint/blob/master/README.md)
- The `value` of `endpoint` should be parsed into `address include host and port`
- Now we don't have a pair operation on the `hdfs address` or tcp-like operation

## Rationale

### Why not use hadoop configuration?

We can specify the configuration of the path via `LoadHadoopConf`
If not specified, the default path `${hadoop_home}/conf` will be used

However, there is no guarantee that the path is wrong, and it is more common for users to use the namenode address directly.

## Compatibility

No compatibility issues at this time.

## Implementation

First add a  `String` type with the name `ProtocolTcp` in `endpoint`. 

```go
const {
    //ProtocolTcp is the file endpoint protocol
    ProtocolTcp = "tcp"
}
```

Then implementing the Tcp method of endpoint

```go
func (p Endpoint) Tcp() (address string) {
	if p.protocol != ProtocolTcp {
		panic(Error{
			Op:       "tcp",
			Err:      ErrInvalidValue,
			Protocol: p.protocol,
			Values:   p.args,
		})
	}
    //It must be host:port
    return p.args.(string)
}
```

Then implementing Parse and NewTcp methods

```go
func Parse(cfg string) (Provider, error) {
	s := strings.Split(cfg, ":")

	switch s[0] {
	.....
	case ProtocolTcp:
        //Handle tcp connection (hdfs)
        return NewTcp(s[1]),nil
	default:
		return Endpoint{}, &Error{"parse", ErrUnsupportedProtocol, s[0], nil}
	}
}
```

```go
func NewTcp(address string) Endpoint {
    return Endpoint{
		protocol: ProtocolTcp,
		args:     path,
	}
}
```