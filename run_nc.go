package main

import (
	"io"
	"strings"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/mmmorris1975/ssm-session-client/datachannel"
)

type bootAgentTmplData struct {
	Cmd      string
	Port     int
	Filename string
}

var bootAgentTmpl = template.Must(template.New("").Parse(
	`sudo su
cd /root/
curl --silent -L -o {{.Cmd}} "https://github.com/fujiwara/tncl/releases/download/v0.0.4/tncl-x86_64-linux-musl"
chmod +x {{.Cmd}}
{{.Cmd}} {{.Port}} > "{{.Filename}}"
`))

func runNc(cfg aws.Config, target string, remoteFile string, port int) (*datachannel.SsmDataChannel, error) {
	buf := &strings.Builder{}
	bootAgentTmpl.Execute(buf, bootAgentTmplData{
		Cmd: "/tmp/tncl", Port: port, Filename: remoteFile})
	cmd := buf.String()

	c := new(datachannel.SsmDataChannel)
	if err := c.Open(cfg, &ssm.StartSessionInput{Target: aws.String(target)}); err != nil {
		return c, err
	}

	r := strings.NewReader(cmd)
	if _, err := io.Copy(c, r); err != nil {
		return c, err
	}

	return c, nil
}

func closeNc(c *datachannel.SsmDataChannel) {
	io.Copy(c, strings.NewReader("exit\n")) // root
	io.Copy(c, strings.NewReader("exit\n")) // ssm-user
	time.Sleep(200 * time.Millisecond)
	_ = c.DisconnectPort()
	_ = c.TerminateSession()
	_ = c.Close()
}
