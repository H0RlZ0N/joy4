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

	conn     *Conn
	demuxer  *rawmedia.Demuxer
	mediaDir rawmedia.MediaDir

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
	if rc.conn != nil {
		rc.conn.Close()
		rc.conn = nil
	}
}

func (rc *RtmpClient) SetMediaDir(mediaDir rawmedia.MediaDir) {
	rc.mediaDir = mediaDir
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

	for {
		select {
		case <-rc.ctx.Done():
			return
		default:
			var pkt av.Packet
			pkt, err = rc.demuxer.ReadPacket()
			if err != nil {
			} else {
				if err = rc.Sendpacket(pkt); err != nil {
					log.Printf("Sendpacket error return\n")
					break
				}
			}
			time.Sleep(time.Nanosecond)
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

	// 视频sps，pps或音频adts未填充，等待填充后再开始传输
	streams := rc.demuxer.GetStreams()
	for _, stream := range streams {
		if rc.mediaDir&rawmedia.MEDIADIR_VIDEO != 0 && stream.StreamType == rawmedia.StreamTypeH264 {
			if stream.CodecData == nil {
				return nil
			}
		} else if rc.mediaDir&rawmedia.MEDIADIR_AUDIOSEND != 0 && stream.StreamType == rawmedia.StreamTypeAdtsAAC {
			if stream.CodecData == nil {
				return nil
			}
		}
	}

	if !rc.initHeader {
		log.Printf("WriteStreamsHeader\n")
		if err = rc.conn.WriteStreamsHeader(streams); err != nil {
			return
		}
		rc.initHeader = true
	}

	if err = rc.conn.WritePacket(pkt); err != nil {
		log.Printf("rtmp WritePacket err: %v\n", err)
		rc.pushSign = false
		return
	}
	return nil
}
