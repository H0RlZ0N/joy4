package rtmp

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/H0RlZ0N/joy4/av"
	"github.com/H0RlZ0N/joy4/format/rawmedia"
)

type RtmpClient struct {
	ctx        context.Context
	cancelfunc context.CancelFunc

	conn    *Conn
	demuxer *rawmedia.Demuxer

	pushSign   bool
	initHeader bool
}

func NewRtmpClient() *RtmpClient {
	return &RtmpClient{}
}

func (rc *RtmpClient) Init(url string) error {
	var err error
	rc.ctx, rc.cancelfunc = context.WithCancel(context.Background())
	rc.conn, err = Dial(url)
	if err != nil {
		return err
	}
	rc.demuxer = rawmedia.NewDemuxer()
	rc.demuxer.FillStreams()
	rc.pushSign = false

	return nil
}

func (rc *RtmpClient) Close() {
	log.Printf("-----------> conn close\n")
	if rc.cancelfunc != nil {
		rc.cancelfunc()
		rc.cancelfunc = nil
	}
	rc.pushSign = false
	rc.initHeader = false
	if rc.conn != nil {
		rc.conn.Close()
		rc.conn = nil
	}
}

func (rc *RtmpClient) WritePacket(packet rawmedia.Packet) error {
	_, err := rc.demuxer.WriteMediaData(packet)
	if err != nil {
		return err
	}
	return nil
}

// start rtmp publish
func (rc *RtmpClient) StartPublish() {
	if rc.conn != nil {
		rc.pushSign = true
	}
}

// stop rtmp publish
func (rc *RtmpClient) StopPublish() {
	rc.pushSign = false
}

func (rc *RtmpClient) Serve() {
	var err error
	tick := time.NewTicker(time.Second * 60)
	defer func() {
		log.Printf("defer close rtmp client\n")
		if tick != nil {
			tick.Stop()
			tick = nil
		}
		rc.Close()
	}()

	for {
		select {
		case <-rc.ctx.Done():
			log.Printf("Done return\n")
			return
		case <-tick.C:
			// 超过60s没有读出数据包，关闭连接
			tick.Stop()
			log.Printf("timeout return\n")
			return
		default:
			var pkt av.Packet
			pkt, err = rc.demuxer.ReadPacket()
			if err != nil {
				//log.Printf("ReadPacket err: %v\n", err)
				tick.Reset(time.Second * 60)
			} else {
				tick.Stop()
				if err = rc.Sendpacket(pkt); err != nil {
					log.Printf("Sendpacket error return\n")
					return
				}
			}
		}
	}
}

func (rc *RtmpClient) Sendpacket(pkt av.Packet) (err error) {
	if !rc.pushSign {
		return nil
	}

	if rc.conn == nil {
		return fmt.Errorf("rtmp disconnect")
	}

	if !rc.initHeader {
		if err = rc.conn.WriteStreamsHeader(rc.demuxer.GetStreams()); err != nil {
			return
		}
		rc.initHeader = true
	}

	// 视频sps，pps或音频adts未填充，等待填充后再开始传输
	streams := rc.demuxer.GetStreams()
	if streams[pkt.Idx].CodecData == nil {
		return nil
	}

	if err = rc.conn.WritePacket(pkt); err != nil {
		log.Printf("rtmp WritePacket err: %v\n", err)
		rc.pushSign = false
		return
	}
	return nil
}
