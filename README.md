# ec2-pc

A file copy command for EC2. It works via SSM without SSH.

## Usage

```
$ ec2-cp <src> <dest>
```

## Example

```sh
$ ec2-cp /path/to/local/file i-i-012345678901abcdef:/path/to/remote/file
```

## Required IAM Policy

* `AmazonSSMManagedInstanceCore`

## Limitations

* tool limitations
  * Copying files from EC2 is not supported
  * Sessions may sometimes remain
* EC2 limitations
  * `ssm-user` needs to be able to become `root`
  * `x86_64` only support
  * Internet access is needed

## Credits

* This program is inspired by [fujiwara/ecsta](https://github.com/fujiwara/ecsta/)
* `port_forwarding.go` is based [mmmorris1975/ssm-session-client](https://github.com/mmmorris1975/ssm-session-client/)
