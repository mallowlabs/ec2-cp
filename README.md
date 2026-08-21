# ec2-cp

A file copy command for EC2. It works via SSM without SSH.

## Usage

```
$ ec2-cp <src> <dest>
```

## Example

```sh
$ ec2-cp /path/to/local/file i-012345678901abcdef:/path/to/remote/file
```

## Required IAM Policy

* `AmazonSSMManagedInstanceCore`

## How It Works

`ec2-cp` doesn't use SCP or SFTP. Instead it opens two independent SSM sessions and bridges a local TCP file transfer through them, then verifies the result with a SHA-256 checksum on both ends:

1. Compute the local file's SHA-256 checksum.
2. Start an SSM shell session and have it download and run [tncl](https://github.com/fujiwara/tncl) (a small netcat-like tool) on the instance, listening on a random port and writing whatever it receives to the destination file.
3. Start a second SSM session using the `AWS-StartPortForwardingSession` document, forwarding a local TCP port to that same port on the instance.
4. Open a local TCP connection to the forwarded port and stream the file into it. A custom reliability layer chunks the data, waits for acknowledgements, and retransmits as needed, since the SSM service silently drops oversized or unacknowledged messages.
5. Once every byte is acknowledged, disconnect the forwarded port so `tncl` sees EOF and exits, then run `sha256sum` on the instance (over the shell session) and compare it against the local checksum.
6. If the checksums don't match (or something failed along the way), retry the whole process, up to 3 attempts.

```mermaid
sequenceDiagram
    actor User
    participant CLI as ec2-cp
    participant Shell as SSM Shell Session
    participant PF as SSM Port Forwarding Session
    participant EC2 as EC2 Instance

    User->>CLI: ec2-cp src target:dest
    CLI->>CLI: compute sha256 of local file

    loop up to 3 attempts
        CLI->>Shell: StartSession, interactive shell
        Shell->>EC2: sudo su, curl tncl, run tncl on port, write to dest
        EC2-->>Shell: echo READY marker
        Shell-->>CLI: remote agent ready

        CLI->>PF: StartSession, AWS-StartPortForwardingSession
        CLI->>CLI: listen on local port

        CLI->>CLI: dial local port from sendFile
        CLI->>PF: relay file bytes, windowed, ack and retransmit
        PF->>EC2: forward bytes to tncl listener
        EC2->>EC2: tncl writes bytes to dest file

        CLI->>PF: wait for all bytes acknowledged
        PF-->>CLI: ack complete
        CLI->>PF: DisconnectToPort
        PF->>EC2: close connection to tncl
        EC2->>EC2: tncl exits
        EC2-->>Shell: echo DONE marker
        Shell-->>CLI: remote transfer finished

        CLI->>Shell: sha256sum dest
        Shell-->>CLI: remote checksum

        alt checksums match
            CLI->>Shell: exit, close sessions
            CLI-->>User: Transfer verified OK
        else mismatch or error
            CLI->>CLI: retry with new random port
        end
    end
```

## Limitations

* tool limitations
  * Copying files from EC2 is not supported
  * Sessions may sometimes remain
* EC2 limitations
  * `ssm-user` needs to be able to become `root`
  * `x86_64` and `arm64` only support
  * Internet access is needed

## Credits

* This program is inspired by [fujiwara/ecsta](https://github.com/fujiwara/ecsta/)
* `port_forwarding.go` is based [mmmorris1975/ssm-session-client](https://github.com/mmmorris1975/ssm-session-client/)
