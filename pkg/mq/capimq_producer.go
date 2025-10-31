package mq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/capillariesio/capillaries/pkg/wfmodel"
)

const CapimqProducerSendTimeout time.Duration = 2000

type CapimqProducer struct {
	url string
}

func NewCapimqProducer(url string) *CapimqProducer {
	return &CapimqProducer{
		url: url,
	}
}

func (p *CapimqProducer) Open() error {
	return nil
}

func (p *CapimqProducer) sendBulkBytes(msgBytes []byte) error {
	req, reqErr := http.NewRequest(http.MethodPost, p.url+"/q/bulk", bytes.NewReader(msgBytes))
	if reqErr != nil {
		return reqErr
	}

	req.Header.Set("content-type", "application/json")

	sendCtx, sendCancel := context.WithTimeout(context.Background(), CapimqProducerSendTimeout*time.Millisecond)
	resp, respErr := http.DefaultClient.Do(req.WithContext(sendCtx))
	sendCancel()
	if respErr != nil {
		return respErr
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cannot send bulk bytes, HTTP response %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

func (p *CapimqProducer) Send(msg *wfmodel.Message) error {
	msgs := make([]*wfmodel.Message, 1)
	msgs[0] = msg
	msgsBytes, marshalErr := json.Marshal(msgs)
	if marshalErr != nil {
		return fmt.Errorf("cannot send one, error when serializing msg: %s", marshalErr.Error())
	}
	return p.sendBulkBytes(msgsBytes)
}

func (p *CapimqProducer) SendBulk(msgs []*wfmodel.Message) error {
	msgsBytes, marshalErr := json.Marshal(msgs)
	if marshalErr != nil {
		return fmt.Errorf("cannot send bulk, error when serializing msgs: %s", marshalErr.Error())
	}
	return p.sendBulkBytes(msgsBytes)
}

func (p *CapimqProducer) Close() error {
	return nil
}

func (p *CapimqProducer) SupportsSendBulk() bool {
	return true
}
